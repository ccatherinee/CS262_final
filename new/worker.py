import time 
import pickle 
import selectors 
import socket
import struct 
import types 
import threading 
import queue 
import random
import sys
import json
from dill.source import getsource 
from pathlib import Path
from constants import * 
from collections import defaultdict


# TODO: fault tolerance, e.g., what happens when try to socket.connect to worker node that is down? 
class Worker: 
    def __init__(self): 
        self.M = self.R = None
        self.mapper = self.reducer = None

        # Map task state (reset upon every completion of map task)
        self.map_task = None # map task number 1-M that this worker is currently working on

        # Reduce task state (reset upon every completion of reduce task)
        self.reduce_task = None # reduce task number 1-R that this worker is currently working on

        self.write_to_master_queue = queue.Queue() # queue of bytes to be sent to master node
        self.request_intermediate_from = queue.Queue() # queue of worker node addresses to request intermediate results from
        self.intermediate_results = queue.Queue() # queue of intermediate map results received from other workers

        # listening socket, through which other workers connect to this worker to request map task results
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.bind(("", random.randint(20000, 30000))) # run worker node on current machine at random port
        self.lsock.listen() 
        self.lsock.setblocking(False) 
        print(f"Worker node listening at {self.lsock.getsockname()}")

        # socket initiates connection to master node
        self.master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master_sock.setblocking(True)
        self.master_sock.connect((MASTER_HOST, MASTER_PORT))
        
        # selector for master socket activity and new workers connecting
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.master_sock, selectors.EVENT_READ | selectors.EVENT_WRITE, data=types.SimpleNamespace(out=[]))
        self.sel.register(self.lsock, selectors.EVENT_READ, data=None) 

        # selector for worker sockets once they have connected
        self.worker_sel = selectors.DefaultSelector()

        self.master_sock.sendall(struct.pack('>I', REQUEST_TASK)) # request a map or reduce task from master node

        # thread for servicing other workers' requests to current worker
        threading.Thread(target=self.service_worker_connection).start()

    def run(self): 
        while True: 
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.fileobj == self.lsock:
                    self.accept_worker_connection() # only worker nodes connect to other workers' listening sockets
                elif key.fileobj == self.master_sock:
                    self.service_master_connection(key, mask)

    def accept_worker_connection(self):
        conn, addr = self.lsock.accept() 
        conn.setblocking(False) 
        self.worker_sel.register(conn, selectors.EVENT_READ, data=None)
        print(f"Worker node listening at {self.lsock.getsockname()} accepted connection from worker node at {addr}")

    def service_worker_connection(self):
        while True:
            events = self.worker_sel.select(timeout=None)
            for key, mask in events:
                sock = key.fileobj
                if mask & selectors.EVENT_READ:
                    raw_opcode = self._recvall(sock, 4)
                    if not raw_opcode:
                        self.worker_sel.unregister(sock)
                        sock.close()
                        continue
                    opcode = struct.unpack('>I', raw_opcode)[0]
                    if opcode == MAP_RESULTS_REQUEST:
                        completed_map_task = struct.unpack('>I', self._recvall(sock, 4))[0]
                        reduce_partition = struct.unpack('>I', self._recvall(sock, 4))[0]
                        # send intermediate results stored in json file to requesting worker
                        intermediate_results = Path(f"mr-{completed_map_task}-{reduce_partition}.json").read_text()
                        sock.sendall(struct.pack('>I', MAP_RESULTS) + struct.pack('>I', completed_map_task) + struct.pack('>I', len(intermediate_results)) + intermediate_results.encode())
                    elif opcode == MAP_RESULTS:
                        # read intermediate results from json file from socket, add to intermediate_results queue
                        completed_map_task = struct.unpack('>I', self._recvall(sock, 4))[0]
                        intermediate_results_len = struct.unpack('>I', self._recvall(sock, 4))[0]
                        intermediate_results = self._recvall(sock, intermediate_results_len).decode()
                        self.intermediate_results.put((completed_map_task, intermediate_results))
                        pass

    def service_master_connection(self, key, mask):
        if mask & selectors.EVENT_READ:
            raw_opcode = self._recvall(self.master_sock, 4)
            if not raw_opcode: # master node is down, so worker nodes should abort
                self.sel.unregister(self.master_sock)
                self.master_sock.close() 
                sys.exit(1)
            opcode = struct.unpack('>I', raw_opcode)[0]
            if opcode == ALL_TASKS_COMPLETE: # all tasks are done, so worker nodes should shut down
                self.sel.unregister(self.master_sock)
                self.master_sock.close() 
                sys.exit(1)
            elif opcode == NO_AVAILABLE_TASK:
                # sleep for 1 second before requesting task again
                time.sleep(1)
                self.master_sock.sendall(struct.pack('>I', REQUEST_TASK))
            elif opcode == MAP_TASK:
                self.map_task = struct.unpack('>I', self._recvall(self.master_sock, 4))[0] # get map task number
                self.M = struct.unpack('>I', self._recvall(self.master_sock, 4))[0]
                mapper_func_len = struct.unpack('>I', self._recvall(self.master_sock, 4))[0]
                mapper_func = self._recvall(self.master_sock, mapper_func_len).decode()
                ldict = {}
                exec(mapper_func, globals(), ldict)
                self.mapper = ldict["mapper"]
                map_task_input_len = struct.unpack('>I', self._recvall(self.master_sock, 4))[0]
                map_task_input = self._recvall(self.master_sock, map_task_input_len).decode()
                threading.Thread(target=self.map_thread, args=(map_task_input,)).start()
            elif opcode == REDUCE_TASK:
                self.reduce_task = struct.unpack('>I', self._recvall(self.master_sock, 4))[0] # get reduce task number
                self.R = struct.unpack('>I', self._recvall(self.master_sock, 4))[0]
                reducer_func_len = struct.unpack('>I', self._recvall(self.master_sock, 4))[0]
                reducer_func = self._recvall(self.master_sock, reducer_func_len).decode()
                ldict = {}
                exec(reducer_func, globals(), ldict)
                threading.Thread(target=self.map_thread).start()
            elif opcode == REDUCE_LOCATION_INFO:
                # another worker at host, port has the result for the specified completed map task
                completed_map_task = struct.unpack('>I', self._recvall(self.master_sock, 4))[0]
                worker_host_len = struct.unpack('>I', self._recvall(self.master_sock, 4))[0]
                worker_host = self._recvall(self.master_sock, worker_host_len).decode()
                worker_port = struct.unpack('>I', self._recvall(self.master_sock, 4))[0]
                self.request_intermediate_from.put((completed_map_task, worker_host, worker_port))
        if mask & selectors.EVENT_WRITE:
            while not self.write_to_master_node_queue.empty(): 
                self.master_sock.sendall(self.write_to_master_node_queue.get() )


    def reduce_thread(self):
        print(f"Worker starting reduce task {self.reduce_task}/{self.R}")

        map_task_results_received = set() # set of map task numbers that this worker has received results for
        map_task_results = defaultdict(list) # maps intermediate key to list of intermediate values for that key
        while len(map_task_results_received) < self.M: # there are still more intermediate results to receive
            if not self.request_intermediate_from.empty():
                completed_map_task, worker_host, worker_port = self.request_intermediate_from.get()
                if completed_map_task not in map_task_results_received: # need to request these map task results from worker
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((worker_host, worker_port))
                        s.sendall(struct.pack('>I', MAP_RESULTS_REQUEST) + struct.pack('>I', completed_map_task) + struct.pack('>I', self.reduce_task))
            if not self.intermediate_results.empty():
                completed_map_task, intermediate_results = self.intermediate_results.get()
                intermediate_results = json.loads(intermediate_results) # list of (key, value) pairs
                for k, v in intermediate_results:
                    map_task_results[k].append(v)
                map_task_results_received.add(completed_map_task)

        print(f"Worker finished shuffle stage of reduce task {self.reduce_task}/{self.R}")
        with open(f"mr-output-{self.reduce_task}.txt", "w") as f:
            for k, vs in map_task_results_received:
                for v in self.reducer(None, k, vs):
                    f.write(f"{k}\t{v}\n")

        print(f"Worker finished reduce task {self.reduce_task}/{self.R}")
        # notify master map task is done and request new task
        self.write_to_master_queue.append(struct.pack('>I', REDUCE_COMPLETE))
        self.reduce_task = None
        self.write_to_master_queue.append(struct.pack('>I', REQUEST_TASK))
        return

    def map_thread(self, map_task_input):
        print(f"Worker starting map task {self.map_task}/{self.M}")
        file_lines = map_task_input.split('\n')
        intermediate_data = defaultdict(list) # key: reduce partition number, value: list of (key, value) pairs
        offset = 0
        for line in file_lines:
            for k, v in self.mapper(None, offset, line):
                intermediate_data[hash(k) % self.R].append((k, v))
            offset += len(line)
        # save each list of key, value pairs into different json files
        for reduce_partition_num, key_value_pairs in intermediate_data.items():
            with open(f"mr-{self.map_task}-{reduce_partition_num}.json", "w") as f:
                json.dump(key_value_pairs, f)
        print(f"Worker finished map task {self.map_task}/{self.M}")
        # notify master map task is done and request new task
        self.write_to_master_queue.append(struct.pack('>I', MAP_COMPLETE))
        self.map_task = None
        self.write_to_master_queue.append(struct.pack('>I', REQUEST_TASK))
        return

    def _recvall(self, sock, n): # receives exactly n bytes from socket, returning None if connection broken
        data = bytearray() 
        while len(data) < n: 
            try: 
                packet = sock.recv(n - len(data))
                if not packet:
                    return None 
            except ConnectionResetError: 
                return None
            data.extend(packet)
        return data 


if __name__ == '__main__':
    Worker().run()
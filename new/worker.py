import time 
import selectors 
import socket
import struct 
import types 
import threading 
import queue 
import random
import sys
import json
import os
import errno
from pathlib import Path
from constants import * 
from collections import defaultdict

 
# Map and reduce worker node class
class Worker: 
    def __init__(self, die_map=False, die_reduce=False): 
        self.die_map, self.die_reduce = die_map, die_reduce # whether to die in map or reduce task, for testing purposes only
        self.bytes_sent = self.bytes_received = 0 # number of bytes sent and received over the network by this worker node, respectively
        
        self.M = self.R = None # number of map and reduce tasks, respectively
        self.mapper = self.reducer = None  # map and reduce functions, respectively

        # Map task state (reset upon every completion of map task)
        self.map_task = None # map task number 1-M that this worker is currently working on

        # Reduce task state (reset upon every completion of reduce task)
        self.reduce_task = None # reduce task number 1-R that this worker is currently working on

        self.write_to_master_queue = queue.Queue() # queue of bytes to be sent to master node
        self.request_intermediate_from = queue.Queue() # queue of worker node addresses to request intermediate results from
        self.intermediate_results = queue.Queue() # queue of intermediate map results received from other workers

        # listening socket, through which other workers connect to this worker to request map task results
        self.listening_port = random.randint(20000, 60000)
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.bind((socket.gethostbyname(socket.gethostname()), self.listening_port)) # run worker node on current machine's IP address at random port
        self.lsock.listen() 
        self.lsock.setblocking(False) 
        print(f"Worker node listening at {self.lsock.getsockname()}")

        # socket initiates connection to master node
        self.master_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master_sock.setblocking(True)
        self.master_sock.connect((MASTER_HOST, MASTER_PORT))
        
        # selector for master socket activity and new workers connecting
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.master_sock, selectors.EVENT_READ | selectors.EVENT_WRITE, data=None)
        self.sel.register(self.lsock, selectors.EVENT_READ, data=None) 

        # read and write selectors for worker sockets once they have connected
        self.worker_read_sel = selectors.DefaultSelector()
        self.worker_write_sel = selectors.DefaultSelector()

        self.write_to_master_queue.put(struct.pack('>Q', REQUEST_TASK)) # request a map or reduce task from master node

        # thread for reading from and writing to other worker nodes
        threading.Thread(target=self.read_worker_connection, daemon=True).start() # daemon thread exits when main worker process/thread exits
        threading.Thread(target=self.write_worker_connection, daemon=True).start()

    # Main thread of worker node: accepts new worker connections and services master node connection
    def run(self): 
        while True: 
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.fileobj == self.lsock:
                    self.accept_worker_connection() # only worker nodes connect to other workers' listening sockets
                elif key.fileobj == self.master_sock:
                    done = self.service_master_connection(key, mask)
                    if done:
                        print(f"Worker node received {self.bytes_received} bytes total and sent {self.bytes_sent} bytes total over the network.")
                        return # exit worker node process if master node down or sent ALL_TASKS_COMPLETE 

    # Accept new, incoming worker connection to this worker node's listening socket
    def accept_worker_connection(self):
        conn, addr = self.lsock.accept() 
        conn.setblocking(False)
        worker_queue = queue.Queue() # queue of bytes to be sent from the current worker node to the connecting worker node
        data_read = types.SimpleNamespace(addr=addr, write_to_worker_queue=worker_queue)
        data_write = types.SimpleNamespace(addr=addr, write_to_worker_queue=worker_queue)
        # register read and write events from the connecting worker socket to the appropriate selectors
        self.worker_read_sel.register(conn, selectors.EVENT_READ, data=data_read)
        self.worker_write_sel.register(conn, selectors.EVENT_WRITE, data=data_write)
        print(f"Worker node accepted connection from {addr}")

    # Thread for reading from other worker nodes
    def read_worker_connection(self):
        while True:
            events = self.worker_read_sel.select(timeout=None)
            for key, mask in events:
                sock, data = key.fileobj, key.data
                if mask & selectors.EVENT_READ:
                    raw_opcode = self._recvall(sock, 8)
                    if raw_opcode is None: # other worker node has disconnected
                        self.worker_read_sel.unregister(sock)
                        self.worker_write_sel.unregister(sock)
                        sock.close()
                        continue
                    opcode = struct.unpack('>Q', raw_opcode)[0]
                    if opcode == MAP_RESULTS_REQUEST: # other worker node is requesting intermediate results from a map task completed on this worker node
                        completed_map_task = struct.unpack('>Q', self._recvall(sock, 8))[0]
                        reduce_partition = struct.unpack('>Q', self._recvall(sock, 8))[0]
                        # send intermediate results stored in json file to requesting worker
                        intermediate_results = Path(f"mr-{completed_map_task}-{reduce_partition}.json").read_bytes()
                        data.write_to_worker_queue.put(struct.pack('>Q', MAP_RESULTS) + struct.pack('>Q', completed_map_task) + struct.pack('>Q', len(intermediate_results)) + intermediate_results)
                    elif opcode == MAP_RESULTS: # other worker node is sending intermediate results from a map task completed on that worker node
                        # read intermediate results from json file from socket, add to intermediate_results queue, to be read in reduce thread
                        completed_map_task = struct.unpack('>Q', self._recvall(sock, 8))[0]
                        intermediate_results_len = struct.unpack('>Q', self._recvall(sock, 8))[0]
                        results = self._recvall(sock, intermediate_results_len)
                        self.intermediate_results.put((completed_map_task, results))
                    else: # should never reach here
                        print("ERROR: Invalid opcode received from another worker node")
                        
    def write_worker_connection(self):
        while True:
            events = self.worker_write_sel.select(timeout=None)
            for key,mask in events:
                sock, data = key.fileobj, key.data
                if mask & selectors.EVENT_WRITE:
                    if not data.write_to_worker_queue.empty():
                        msg = data.write_to_worker_queue.get()
                        sock.sendall(msg)
                        self.bytes_sent += len(msg)

    def service_master_connection(self, key, mask):
        if mask & selectors.EVENT_READ:
            raw_opcode = self._recvall(self.master_sock, 8)
            if not raw_opcode: # master node is down, so worker nodes should abort
                self.sel.unregister(self.master_sock)
                self.master_sock.close() 
                return True # signal that worker node should exit
            opcode = struct.unpack('>Q', raw_opcode)[0]
            if opcode == ALL_TASKS_COMPLETE: # all tasks are done, so worker nodes should shut down
                self.sel.unregister(self.master_sock)
                self.master_sock.close() 
                return True # signal worker node should exit
            elif opcode == NO_AVAILABLE_TASK:
                # sleep for 1 second before requesting task again
                time.sleep(1)
                self.master_sock.sendall(struct.pack('>Q', REQUEST_TASK))
                self.bytes_sent += 8
            elif opcode == MAP_TASK:
                self.map_task = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0] # get map task number
                self.M = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                print(f"Worker node receiving map task {self.map_task}/{self.M} from master node")
                self.R = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                mapper_func_len = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                mapper_func = self._recvall(self.master_sock, mapper_func_len).decode()
                ldict = {}
                exec(mapper_func, globals(), ldict)
                self.mapper = ldict["mapper"]
                map_task_input_len = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                map_task_input = self._recvall(self.master_sock, map_task_input_len).decode()
                print(f"Worker node received map task input file from master node")
                threading.Thread(target=self.map_thread, args=(map_task_input,)).start() # start thread for map task
            elif opcode == REDUCE_TASK:
                self.reduce_task = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0] # get reduce task number
                self.M = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                self.R = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                print(f"Worker node receiving reduce task {self.reduce_task}/{self.R} from master node")
                reducer_func_len = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                reducer_func = self._recvall(self.master_sock, reducer_func_len).decode()
                ldict = {}
                exec(reducer_func, globals(), ldict)
                self.reducer = ldict["reducer"]
                threading.Thread(target=self.reduce_thread).start() # start thread for reduce task
            elif opcode == REDUCE_LOCATION_INFO:
                # another worker at host, port has the result for the specified completed map task
                completed_map_task = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                worker_host_len = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                worker_host = self._recvall(self.master_sock, worker_host_len).decode()
                worker_port = struct.unpack('>Q', self._recvall(self.master_sock, 8))[0]
                self.request_intermediate_from.put((completed_map_task, worker_host, worker_port))
            else:
                print("ERROR: Invalid opcode received from master node")
        if mask & selectors.EVENT_WRITE:
            while not self.write_to_master_queue.empty(): 
                msg = self.write_to_master_queue.get()
                self.master_sock.sendall(msg)
                self.bytes_sent += len(msg)
        return False # signal that worker node should not exit

    # Reduce thread for worker node, started when worker node receives REDUCE_TASK opcode from master node
    def reduce_thread(self):
        print(f"Worker starting reduce task {self.reduce_task}/{self.R}")
        if self.die_reduce: # worker node should die during reduce task for testing
            print(f"Worker node died during reduce task {self.reduce_task}/{self.R}")
            os._exit(1)
        map_task_results_received = set() # set of map task numbers that this worker has received results for
        map_task_results = defaultdict(list) # maps intermediate key to list of intermediate values for that key
        while len(map_task_results_received) < self.M: # there are still more intermediate results 1-M to receive
            if not self.request_intermediate_from.empty(): # request intermediate results from other workers
                completed_map_task, worker_host, worker_port = self.request_intermediate_from.get()
                if completed_map_task not in map_task_results_received: # we don't have this map task's results yet
                    try:
                        # connect to toher worker node
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect((worker_host, worker_port))
                        request_queue = queue.Queue()
                        # send request for results from completed map task
                        request_queue.put(struct.pack('>Q', MAP_RESULTS_REQUEST) + struct.pack('>Q', completed_map_task) + struct.pack('>Q', self.reduce_task))
                        self.worker_read_sel.register(s, selectors.EVENT_READ, data=None)
                        self.worker_write_sel.register(s, selectors.EVENT_WRITE, data=types.SimpleNamespace(write_to_worker_queue=request_queue))
                        print(f"Worker node sent request for results from map task {completed_map_task}")
                    except (ConnectionRefusedError, TimeoutError): # other worker node is down
                        pass
            if not self.intermediate_results.empty(): # received intermediate results from other worker nodes
                completed_map_task, intermediate_results = self.intermediate_results.get()
                intermediate_results = json.loads(intermediate_results) # list of (key, value) pairs
                for k, v in intermediate_results:
                    map_task_results[k].append(v) # track list of intermediate values for each intermediate key
                map_task_results_received.add(completed_map_task)
                print(f"Worker node received results from map task {completed_map_task}")

        print(f"Worker finished shuffle stage of reduce task {self.reduce_task}/{self.R}")
        # write intermediate results to file
        with open(f"mr-output-{self.reduce_task}.txt", "w") as f:
            for k, vs in map_task_results.items():
                for v in self.reducer(None, k, vs):
                    f.write(f"{k}\t{v}\n")

        print(f"Worker finished reduce task {self.reduce_task}/{self.R}")
        # notify master map task is done and request new task
        self.write_to_master_queue.put(struct.pack('>Q', REDUCE_COMPLETE))
        self.reduce_task = None
        self.write_to_master_queue.put(struct.pack('>Q', REQUEST_TASK))
        return

    # Map thread for worker node, started when worker node receives MAP_TASK opcode from master node
    def map_thread(self, map_task_input):
        print(f"Worker starting map task {self.map_task}/{self.M}")
        if self.die_map: # worker node should die during map task for testing
            print(f"Worker node died during map task {self.map_task}/{self.M}")
            os._exit(1)
        file_lines = map_task_input.split('\n')
        intermediate_data = defaultdict(list) # key: reduce partition number, value: list of (key, value) pairs belonging to that partition
        offset = 0
        for line in file_lines:
            for k, v in self.mapper(None, offset, line): # each line of the input text file has key=file offset, value=line contents
                intermediate_data[hash(k) % self.R + 1].append((k, v)) # ensure key is hashed to reduce partition in 1-R
            offset += len(line)
        # save each list of key, value pairs into different json files, saving an empty file if no data belongs to that reduce partition
        for reduce_partition_num in range(1, self.R + 1):
            with open(f"mr-{self.map_task}-{reduce_partition_num}.json", "w") as f:
                json.dump(intermediate_data[reduce_partition_num], f)
        print(f"Worker finished map task {self.map_task}/{self.M}")
        # notify master map task is done and request new task
        self.write_to_master_queue.put(struct.pack('>Q', MAP_COMPLETE) + struct.pack('>Q', self.listening_port))
        self.map_task = None
        self.write_to_master_queue.put(struct.pack('>Q', REQUEST_TASK))
        return

    def _recvall(self, sock, n): # receives exactly n bytes from socket, returning None if connection broken
        data = bytearray() 
        while len(data) < n: 
            try: 
                packet = sock.recv(min(4096, n - len(data))) # call sock.recv with up to 4096 bytes at a time for efficiency
                if not packet:
                    return None 
            except ConnectionResetError: 
                return None
            data.extend(packet)
        self.bytes_received += n
        return data 

# Sockets and network buffers behave differently on macOS compared to Linux
# This code modifies sock.sendall to not immediately error when the network buffer is full but instead wait a bit and try again
if "darwin" == sys.platform: 
    def socket_socket_sendall(self, data):
        while len(data) > 0:
            try:
                bytes_sent = self.send(data)
                data = data[bytes_sent:]
            except socket.error as e:
                if e.errno == errno.EAGAIN:
                    time.sleep(0.1)
                else:
                    raise e
    socket.socket.sendall = socket_socket_sendall


if __name__ == '__main__':
    # if die_map or die_reduce is passed as an argument, the worker node will die during the map or reduce task respectively, for testing purposes
    if len(sys.argv) > 1:
        if sys.argv[1] == "die_map":
            Worker(die_map=True).run()
        elif sys.argv[1] == "die_reduce":
            Worker(die_reduce=True).run()
    else:
        Worker().run() # otherwise, worker node will run normally
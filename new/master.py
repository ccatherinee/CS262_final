import time 
import selectors 
import socket
import struct 
import types 
import queue 
import sys
import errno
from dill.source import getsource 
from pathlib import Path
from constants import * 
from collections import defaultdict


# MapReduce Job class, i.e., the master node class
class MRJob: 
    def __init__(self, M, R):
        self.M, self.R = M, R # the number of map and reduce jobs respectively
        self.initial_delay = INITIAL_DELAY # whether to delay assigning tasks for 10 seconds to allow all workers to connect first, for testing purposes only

        self.worker_connections = {} # maps worker node address to socket connection

        self.available_map_tasks = queue.Queue() # map tasks that are ready to be assigned to workers
        self.available_reduce_tasks = queue.Queue() # reduce tasks that are ready to be assigned to workers
        # initially, all M + R jobs are available
        for i in range(1, M + 1):
            self.available_map_tasks.put(i)
        for i in range(1, R + 1):
            self.available_reduce_tasks.put(i)

        self.in_progress_map_tasks = {} # maps worker node address to map task number
        self.in_progress_reduce_tasks = {} # maps worker node address to reduce task number

        self.completed_map_tasks = defaultdict(list) # maps worker node address to list of completed map task numbers
        self.completed_map_tasks_locations = {} # maps completed map task numbers to worker node LISTENING SOCKET address who completed it
        self.completed_tasks = 0 # number of total completed tasks, out of M + R
        
    def mapper(self, key, value): # map function, overwritten by user
        return
    
    def reducer(self, key, value): # reduce function, overwritten by user
        return
    
    # Main thread of master node: sets up listening socket and communicates with worker nodes
    def run(self): 
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # listening socket, through which workers connect to master
        self.lsock.bind((MASTER_HOST, MASTER_PORT)) # run master node on user machine
        self.lsock.listen() 
        self.lsock.setblocking(False) 
        print(f"Master node listening at {self.lsock.getsockname()}")

        self.sel = selectors.DefaultSelector() # selector for listening socket and worker sockets
        self.sel.register(self.lsock, selectors.EVENT_READ, data=None) 

        while True: 
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.data is None: 
                    self.accept_worker_connection() # accept new worker connection
                else: 
                    if self.initial_delay:
                        time.sleep(10) # delay initial assigning of tasks for 10 seconds to allow all workers to connect first
                        self.initial_delay = False
                    done = self.service_worker_connection(key, mask) # service existing worker connection
                    if done:
                        return # exit out of master.run() if all tasks are complete, returning control to user program

    # Accept a new, incoming worker connection to the master node's listening socket
    def accept_worker_connection(self): 
        conn, addr = self.lsock.accept() 
        conn.setblocking(False)
        # store queue of messages to send to worker node and worker node address in data field of selector
        data = types.SimpleNamespace(write_to_worker_queue=queue.Queue(), worker_addr=addr)
        self.sel.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)
        self.worker_connections[addr] = conn
        print(f"Master node accepted connection from worker node at {addr}")

    # Service an existing worker connection, i.e., receive and send messages to worker node
    def service_worker_connection(self, key, mask):
        sock, data = key.fileobj, key.data
        worker_addr = data.worker_addr
        done = False # whether all tasks are complete
        if mask & selectors.EVENT_READ:
            raw_opcode = self._recvall(sock, 8)
            if raw_opcode is None: 
                print(f"Master node detected worker node at {worker_addr} has disconnected")
                self.worker_connections.pop(worker_addr) # remove worker node from list of worker nodes
                # re-make available map tasks that were completed by disconnected worker node
                for task in self.completed_map_tasks[worker_addr]:
                    self.available_map_tasks.put(task)
                    self.completed_map_tasks_locations.pop(task)
                    self.completed_tasks -= 1
                self.completed_map_tasks.pop(worker_addr)
                # re-make available map and reduce tasks that were in progress by disconnected worker node
                if worker_addr in self.in_progress_map_tasks:
                    self.available_map_tasks.put(self.in_progress_map_tasks[worker_addr])
                    self.in_progress_map_tasks.pop(worker_addr)
                if worker_addr in self.in_progress_reduce_tasks:
                    self.available_reduce_tasks.put(self.in_progress_reduce_tasks[worker_addr])
                    self.in_progress_reduce_tasks.pop(worker_addr)
                # remove the disconnected worker node from the selector
                self.sel.unregister(sock)
                sock.close() 
                return done
            
            opcode = struct.unpack('>Q', raw_opcode)[0]
            if opcode == REQUEST_TASK: # worker node requests a task
                if not self.available_map_tasks.empty(): # assign available map task to worker node
                    task = self.available_map_tasks.get()
                    print(f"Master node assigning map task {task}/{self.M} to worker node at {worker_addr}")
                    mapper_func_str = getsource(self.mapper).strip()
                    map_task_input = Path(f"mr-input-{task}.txt").read_bytes() # map task input is one text file
                    # send worker node map task number, number of map tasks, number of reduce tasks, mapper function, and map task input
                    data.write_to_worker_queue.put(struct.pack('>Q', MAP_TASK) + struct.pack('>Q', task) + struct.pack('>Q', self.M) + struct.pack('>Q', self.R) + struct.pack('>Q', len(mapper_func_str)) + mapper_func_str.encode() + struct.pack('>Q', len(map_task_input)) + map_task_input)
                    self.in_progress_map_tasks[worker_addr] = task
                elif not self.available_reduce_tasks.empty(): # assign available reduce task to worker node
                    task = self.available_reduce_tasks.get()
                    print(f"Master node assigning reduce task {task}/{self.R} to worker node at {worker_addr}")
                    reducer_func_str = getsource(self.reducer).strip()
                    # send worker node reduce task number, number of map tasks, number of reduce tasks, reducer function
                    data.write_to_worker_queue.put(struct.pack('>Q', REDUCE_TASK) + struct.pack('>Q', task) + struct.pack('>Q', self.M) + struct.pack('>Q', self.R) + struct.pack('>Q', len(reducer_func_str)) + reducer_func_str.encode())
                    self.in_progress_reduce_tasks[worker_addr] = task
                    # send this new reduce worker the locations of previously completed map tasks
                    for completed_map_task, map_worker_addr in self.completed_map_tasks_locations.items():
                        data.write_to_worker_queue.put(struct.pack('>Q', REDUCE_LOCATION_INFO) + struct.pack('>Q', completed_map_task) + struct.pack('>Q', len(map_worker_addr[0])) + map_worker_addr[0].encode() + struct.pack('>Q', map_worker_addr[1]))
                elif self.completed_tasks < self.M + self.R: # no tasks currently available
                    data.write_to_worker_queue.put(struct.pack('>Q', NO_AVAILABLE_TASK))
                elif self.completed_tasks == self.M + self.R: # all tasks completed
                    data.write_to_worker_queue.put(struct.pack('>Q', ALL_TASKS_COMPLETE))
            elif opcode == MAP_COMPLETE: # worker node has completed a map task
                # update internal master node state tracking completed tasks and their locations
                completed_map_task = self.in_progress_map_tasks.pop(worker_addr)
                print(f"Master node received completed map task {completed_map_task} from worker node at {worker_addr}")
                self.completed_map_tasks[worker_addr].append(completed_map_task)
                worker_listening_port = struct.unpack('>Q', self._recvall(sock, 8))[0]
                self.completed_map_tasks_locations[completed_map_task] = (worker_addr[0], worker_listening_port)
                self.completed_tasks += 1
                # send workers currently working on reduce task the location of this map task's results
                for reduce_worker_addr in self.in_progress_reduce_tasks:
                    self.worker_connections[reduce_worker_addr].sendall(struct.pack('>Q', REDUCE_LOCATION_INFO) + struct.pack('>Q', completed_map_task) + struct.pack('>Q', len(worker_addr[0])) + worker_addr[0].encode() + struct.pack('>Q', worker_listening_port))
            elif opcode == REDUCE_COMPLETE: # worker node has completed a reduce task
                completed_reduce_task = self.in_progress_reduce_tasks.pop(worker_addr)
                print(f"Master node received completed reduce task {completed_reduce_task} from worker node at {worker_addr}")
                self.completed_tasks += 1
                if self.completed_tasks == self.M + self.R: # all tasks completed
                    for conn in self.worker_connections.values(): # tell all worker nodes to terminate
                        conn.sendall(struct.pack('>Q', ALL_TASKS_COMPLETE))
                        self.sel.unregister(conn)
                        conn.close()
                    done = True # tell master.run to terminate

        if mask & selectors.EVENT_WRITE: # worker node is ready to receive data
            if not data.write_to_worker_queue.empty(): 
                sock.sendall(data.write_to_worker_queue.get()) # send data to worker node

        return done # return whether master.run should terminate / whether all tasks are done

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
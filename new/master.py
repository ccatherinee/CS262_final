import time 
import pickle 
import selectors 
import socket
import struct 
import types 
import threading 
import queue 
import random
from dill.source import getsource 
from pathlib import Path
from constants import * 
from collections import defaultdict


class MRJob: 
    def __init__(self, M, R):
        self.M, self.R = M, R

        self.worker_connections = {} # maps worker node address to socket connection
        # self.worker_out = {} # maps socket connection to out bytes to be sent to worker node

        self.available_map_tasks = queue.Queue() # map tasks that are ready to be assigned to workers
        self.available_reduce_tasks = queue.Queue() # reduce tasks that are ready to be assigned to workers
        for i in range(1, M + 1):
            self.available_map_tasks.put(i)
        for i in range(1, R + 1):
            self.available_reduce_tasks.put(i)

        self.in_progress_map_tasks = {} # maps worker node address to map task number
        self.in_progress_reduce_tasks = {} # maps worker node address to reduce task number

        self.completed_map_tasks = defaultdict(list) # maps worker node address to list of completed map task numbers
        self.completed_map_tasks_locations = {} # maps completed mask task numbers to worker node listening socket address who completed it
        self.completed_tasks = 0 # number of total completed tasks, out of M + R

        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # listening socket, through which workers connect to master
        self.lsock.bind((MASTER_HOST, MASTER_PORT)) # run master node on user machine
        self.lsock.listen() 
        self.lsock.setblocking(False) 
        print(f"Master node listening at {self.lsock.getsockname()}")

        self.sel = selectors.DefaultSelector() # selector for listening socket and worker sockets
        self.sel.register(self.lsock, selectors.EVENT_READ, data=None) 
        
    def mapper(self, key, value): # overwritten by user
        return
    
    def reducer(self, key, value): # overwritten by user
        return
    
    def run(self): 
        while True: 
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.data is None: 
                    self.accept_worker_connection() # accept new worker connection
                else: 
                    done = self.service_worker_connection(key, mask) # service existing worker connection
                    if done:
                        return # exit out of master.run if all tasks are complete

    def accept_worker_connection(self): 
        conn, addr = self.lsock.accept() 
        conn.setblocking(False) 
        data = types.SimpleNamespace(out=[])
        self.sel.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)
        self.worker_connections[addr] = conn
        # self.worker_out[conn] = data.out
        print(f"Master node accepted connection from worker node at {addr}")

    def service_worker_connection(self, key, mask):
        sock, data = key.fileobj, key.data
        if mask & selectors.EVENT_READ:
            raw_opcode = self._recvall(sock, 4)
            if not raw_opcode: 
                # TODO: deal with dead worker
                self.sel.unregister(sock)
                sock.close() 
                return
            
            opcode, worker_addr = struct.unpack('>I', raw_opcode)[0], sock.getpeername()
            if opcode == REQUEST_TASK:
                if not self.available_map_tasks.empty(): # assign map task to worker node
                    task = self.available_map_tasks.get()
                    print(f"Master node assigning map task {task}/{self.M} to worker node at {worker_addr}")
                    mapper_func_str = getsource(self.mapper).strip()
                    map_task_input = Path(f"mr-input-{task}.txt").read_text() # map task input is one text file
                    sock.sendall(struct.pack('>I', MAP_TASK) + struct.pack('>I', task) + struct.pack('>I', self.M) + struct.pack('>I', self.R) + struct.pack('>I', len(mapper_func_str)) + mapper_func_str.encode() + struct.pack('>I', len(map_task_input)) + map_task_input.encode())
                    self.in_progress_map_tasks[worker_addr] = task
                elif not self.available_reduce_tasks.empty(): # assign reduce task to worker node
                    task = self.available_reduce_tasks.get()
                    print(f"Master node assigning reduce task {task}/{self.R} to worker node at {worker_addr}")
                    reducer_func_str = getsource(self.reducer).strip()
                    sock.sendall(struct.pack('>I', REDUCE_TASK) + struct.pack('>I', task) + struct.pack('>I', self.M) + struct.pack('>I', self.R) + struct.pack('>I', len(reducer_func_str)) + reducer_func_str.encode())
                    self.in_progress_reduce_tasks[worker_addr] = task
                    # send this new reduce worker the locations of previously completed map tasks
                    for completed_map_task, map_worker_addr in self.completed_map_tasks_locations.items():
                        sock.sendall(struct.pack('>I', REDUCE_LOCATION_INFO) + struct.pack('>I', completed_map_task) + struct.pack('>I', len(map_worker_addr[0])) + map_worker_addr[0].encode() + struct.pack('>I', map_worker_addr[1]))
                elif self.completed_tasks < self.M + self.R: # no tasks currently available
                    sock.sendall(struct.pack('>I', NO_AVAILABLE_TASK))
                elif self.completed_tasks == self.M + self.R: # all tasks completed
                    sock.sendall(struct.pack('>I', ALL_TASKS_COMPLETE))
            elif opcode == MAP_COMPLETE:
                completed_map_task = self.in_progress_map_tasks.pop(worker_addr)
                print(f"Master node received completed map task {completed_map_task} from worker node at {worker_addr}")
                self.completed_map_tasks[worker_addr].append(completed_map_task)
                worker_listening_port = struct.unpack('>I', self._recvall(sock, 4))[0]
                self.completed_map_tasks_locations[completed_map_task] = (worker_addr[0], worker_listening_port)
                self.completed_tasks += 1
                # send workers currently working on reduce task the location of this map task's results
                for reduce_worker_addr in self.in_progress_reduce_tasks:
                    self.worker_connections[reduce_worker_addr].sendall(struct.pack('>I', REDUCE_LOCATION_INFO) + struct.pack('>I', completed_map_task) + struct.pack('>I', len(worker_addr[0])) + worker_addr[0].encode() + struct.pack('>I', worker_addr[1]))
            elif opcode == REDUCE_COMPLETE:
                completed_reduce_task = self.in_progress_reduce_tasks.pop(worker_addr)
                print(f"Master node received completed reduce task {completed_reduce_task} from worker node at {worker_addr}")
                self.completed_tasks += 1
                if self.completed_tasks == self.M + self.R:
                    for conn in self.worker_connections.values():
                        conn.sendall(struct.pack('>I', ALL_TASKS_COMPLETE))
                        self.sel.unregister(conn)
                        conn.close()
                    return True # signal that master.run should terminate now
        return False # signal that master.run should not terminate, tasks are not all done

        # if mask & selectors.EVENT_WRITE:
        #     if len(data.out) > 0: # send all out data to worker node
        #         while len(data.outb) > 0: 
        #             sock.sendall(data.outb.pop(0))

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

    

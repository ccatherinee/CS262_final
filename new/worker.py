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
from dill.source import getsource 
from pathlib import Path
from constants import * 
from collections import defaultdict

# TODO: fault tolerance
# what happens when try to socket.connect to worker node that is down? 

# needs to send request_task after sending map_complete or reduce_complete

# shutdown process on all tasks complete

# might start reduce task before all map tasks finish - worker is responsible for ensuring 
# when it has all required map information through shuffling, before sorting and performing reduce


class Worker: 
    def __init__(self): 
        self.M = self.R = None
        self.mapper = self.reducer = None

        # Map task state (reset upon every completion of map task)
        self.map_task = None

        # Reduce task state (reset upon every completion of reduce task)
        self.reduce_task = None # reduce task number 1-R that this worker is currently working on
        self.map_task_results_received = set() # set of map task numbers that this worker has received results for
        self.map_task_results = defaultdict(list) # maps intermediate key to list of intermediate values for that key

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
        
        # selector for master socket activity and activity from worker sockets, once they reach out to connect
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.master_sock, selectors.EVENT_READ | selectors.EVENT_WRITE, data=types.SimpleNamespace(out=[]))
        self.sel.register(self.lsock, selectors.EVENT_READ, data=None) 

        self.master_sock.sendall(struct.pack('>I', REQUEST_TASK)) # request a map or reduce task from master node

        # TODO: threads needed: thread for mapping when receive map_task
        # thread for reducing after receive reduce_task and collected all relevant intermediary data
        # thread for constantly communicating with other worker nodes - this is just main thread!
    def run(self): 
        while True: 
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.fileobj == self.lsock:
                    self.accept_worker_connection() # only worker nodes connect to other workers' listening sockets
                elif key.fileobj == self.master_sock:
                    self.service_master_connection(key, mask)
                else: 
                    self.service_worker_connection(key, mask)

    def accept_worker_connection(self):
        conn, addr = self.lsock.accept() 
        conn.setblocking(False) 
        self.sel.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data=None)
        print(f"Worker node listening at {self.lsock.getsockname()} accepted connection from worker node at {addr}")

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
                pass
            elif opcode == MAP_TASK:
                pass
            elif opcode == REDUCE_TASK:
                pass
            elif opcode == REDUCE_LOCATION_INFO:
                pass
        if mask & selectors.EVENT_WRITE:
            # send key.data?
            pass

    def service_worker_connection(self, key, mask):
        pass

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
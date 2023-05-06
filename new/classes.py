import time 
from constants import * 
import pickle 
from dill.source import getsource 
import selectors 
import socket
import struct 
import time 
from multiprocessing import Process
import types 
import threading 
import queue 
from os import kill, getpid 
from signal import SIGKILL


class MRJob: 
    def __init__(self, n, WORKER_PORT_START): 
        self.master_node = MasterNode(n, WORKER_PORT_START) 
    
    def mapper(self, key, value): 
        yield key, value 
    
    def reducer(self, key, value): 
        yield key, value 
    
    def run(self, inputs): 
        self.master_node.run(inputs, self.mapper, self.reducer)
    

class MasterNode: 
    def __init__(self, n, WORKER_PORT_START): 
        # selector which monitors connections to worker nodes 
        self.sel = selectors.DefaultSelector()

        # maps host, port to (input, hash, data.outb, sock)
        self.worker_info = {}
        self.heartbeat_queue = queue.Queue()
        self.worker_confirmations_queue = queue.Queue()
        self.dead_workers = {}

        threading.Thread(target=self.monitor_workers_thread).start() 

        # spawn up n servers
        for hash_ in range(n): 
            if hash_ == 0: 
                self.start_worker("", WORKER_PORT_START + hash_, hash_, True)
            else: 
                self.start_worker("", WORKER_PORT_START + hash_, hash_, False)
        threading.Thread(target=self.heartbeat_thread).start() 

    def start_worker(self, host, port, hash_, kill_process): 
        worker_process = Process(target=Worker(host, port, hash_, kill_process).run, args=())
        worker_process.start() 
        time.sleep(1)

        new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        new_sock.connect((host, port))
        print("Master node connecting to ", port)
        
        data = types.SimpleNamespace(outb=[])
        # !!! register connection to worker 
        self.sel.register(new_sock, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)
        self.worker_info[("", port)] = [None, hash_, data.outb, new_sock]

    def split(self, a, n): 
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

    def mapper_setup(self, k): 
        input_ = self.worker_info[k][0]
        mapper_str = getsource(self.mapper) 
        mapper_str = mapper_str.strip() 
        to_send = self._pack_n_args(MAP, [mapper_str], pickle.dumps(input_))
        self.worker_info[k][2].append(to_send)

    def reducer_setup(self, k): 
        reducer_str = getsource(self.reducer)
        reducer_str = reducer_str.strip() 
        worker_locs = list(self.worker_info.keys())
        to_send = self._pack_n_args(REDUCE, [reducer_str], pickle.dumps(worker_locs))
        self.worker_info[k][2].append(to_send)

    
    def heartbeat_thread(self): 
        while True: 
            current = list(self.worker_info.keys())
            start_time = time.time() 
            for loc in current: 
                outb = self.worker_info[loc][2]
                outb.append(struct.pack('>I', HEARTBEAT))
                try: 
                    conf = self.heartbeat_queue.get(block=True, timeout=10)
                except queue.Empty: 
                    print(f"Master node detected worker node at {loc} is dead")
                    dead_worker_info = self.worker_info[loc]
                    del self.worker_info[loc]
                    self.dead_workers[loc] = dead_worker_info
            time_elapsed = time.time() - start_time 
            if time_elapsed < 10: 
                time.sleep(10 - time_elapsed)

    def monitor_workers_thread(self): 
        while True: 
            events = self.sel.select(timeout=None) 
            for key, mask in events:
                sock, data = key.fileobj, key.data
                if mask & selectors.EVENT_READ:
                    raw_opcode = self._recvall(sock, 4)
                    if not raw_opcode: 
                        self.sel.unregister(sock)
                        sock.close() 
                        continue
                    opcode = struct.unpack('>I', raw_opcode)[0]
                    loc = sock.getpeername()
                    if opcode == HEARTBEAT_CONFIRM: 
                        self.heartbeat_queue.put(loc)
                    else: 
                        self.worker_confirmations_queue.put((opcode, loc))
                if (mask & selectors.EVENT_WRITE) and len(data.outb) > 0:
                    while len(data.outb) > 0: 
                        temp = data.outb.pop(0)
                        sock.sendall(temp)
    
    def wait_until_confirmation(self, current_opcode): 
        unconfirmed = list(self.worker_info.keys())
        while len(unconfirmed) > 0: 
            if not self.worker_confirmations_queue.empty(): 
                opcode, loc = self.worker_confirmations_queue.get() 
                if loc[0] == '127.0.0.1': 
                    loc = ('', loc[1])
                if loc in unconfirmed and opcode == current_opcode: 
                    unconfirmed.remove(loc)
            if len(self.dead_workers) > 0: 
                for (dead_host, dead_port), v in self.dead_workers.items(): 
                    unconfirmed.remove((dead_host, dead_port))
                    input_, hash_, outb, sock = v

                    new_host, new_port = "", dead_port + len(self.worker_info) + 1
                    new_loc = (new_host, new_port)
                    self.start_worker(new_host, new_port, hash_, False)
                    unconfirmed.append(new_loc)
                    self.worker_info[new_loc][0] = input_
                    # if the current opcode is MAP_CONFIRM, then we're still on the mapping stage, so just send out the mapping 
                    self.mapper_setup(new_loc)
                    if current_opcode == REDUCE_CONFIRM: 
                        self.reducer_setup(new_loc)
                        # send UPDATE message
                        for loc in self.worker_info.keys(): 
                            if loc == new_loc: continue 
                            to_send = self._pack_n_args(UPDATE, [], pickle.dumps([(dead_host, dead_port), new_loc]))
                            self.worker_info[loc][2].append(to_send)
                self.dead_workers = {}
                
    def run(self, inputs, mapper, reducer): 
        self.mapper, self.reducer = mapper, reducer 
        
        idx = 0 
        inputs = list(self.split(inputs, len(self.worker_info)))
        for k in self.worker_info.keys(): 
            self.worker_info[k][0] = inputs[idx]
            idx += 1

        for loc in self.worker_info.keys(): 
            self.mapper_setup(loc) 
        self.wait_until_confirmation(MAP_CONFIRM)

        for loc in self.worker_info.keys(): 
            self.reducer_setup(loc) 
        self.wait_until_confirmation(REDUCE_CONFIRM)

    def _recvall(self, sock, n):
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

    def _pack_n_args(self, opcode, reg_args, pickled=None): 
        to_send = struct.pack('>I', opcode)
        for arg in reg_args: 
            to_send += struct.pack('>I', len(arg)) + arg.encode("utf-8")
        if pickled: 
            to_send += struct.pack('>I', len(pickled)) + pickled
        return to_send 

    def _recv_n_args(self, sock, n, pickled=None): 
        args = []
        for _ in range(n): 
            arg_len = struct.unpack('>I', self._recvall(sock, 4))[0]
            args.append(self._recvall(sock, arg_len).decode("utf-8", "strict"))
        if pickled: 
            pickle_len = struct.unpack('>I', self._recvall(sock, 4))[0]
            raw_pickle = self._recvall(sock, pickle_len)
            pickled_obj = pickle.loads(raw_pickle)
            args.append(pickled_obj)
        return args


class Worker: 
    def __init__(self, host, port, hash_, kill_process): 
        # host and port the worker is at 
        self.host, self.port = host, port 
        # the hash this worker is responsible for 
        self.hash = hash_ 
        # maps hash to (hashes to list of (k, v))
        self.state = None 
        # maps (host, port) to the list of (k, v) you've received from that (host, port) 
        self.receive_state = {}
        self.reduce_state = {}
        self.kill_process = kill_process
    
    def accept_wrapper(self): 
        conn, addr = self.lsock.accept() 
        conn.setblocking(False) 
        if self.state == None: 
            data = types.SimpleNamespace(addr=addr)
            self.sel.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)
            self.master_node_conn = conn 
            print("Accepting the master node.")
        else: 
            data = types.SimpleNamespace(addr=addr, outb=b"")
            self.workers_sel.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)

    def communication_with_workers(self): 
        while True: 
            events = self.workers_sel.select(timeout=None) 
            for key, mask in events: 
                sock, data = key.fileobj, key.data

                if mask & selectors.EVENT_READ:
                    raw_opcode = self._recvall(sock, 4)
                    if not raw_opcode: 
                        self.workers_sel.unregister(sock)
                        sock.close() 
                        continue 
                    opcode = struct.unpack('>I', raw_opcode)[0]
                    if opcode == REQUEST: 
                        raw_hash = self._recvall(sock, 4)
                        hash_ = struct.unpack('>I', raw_hash)[0]
                        if not hash_ in self.state: 
                            self.state[hash_] = []
                        data.outb += self._pack_n_args(GIVE, [], pickle.dumps(self.state[hash_]))

                    elif opcode == GIVE: 
                        keys_and_values = self._recv_n_args(sock, 0, True)[0]
                        host, port = sock.getpeername()
                        if host == '127.0.0.1': host = ''
                        self.receive_state[(host, port)][1] = keys_and_values 

                        res = [self.receive_state[k][1] for k in self.receive_state.keys()]
                        if not None in res: 
                            # self.receive_state: {(host, port): [socket, list]}
                            for _, list_ in self.receive_state.items(): 
                                for k, v in list_[1]: 
                                    if k not in self.reduce_state:
                                        self.reduce_state[k] = []
                                    self.reduce_state[k].append(v)
                            ldict = {}
                            exec(self.reducer, globals(), ldict)
                            reducer = "reducer"
                            for k1, v1 in self.reduce_state.items(): 
                                for k2, v2 in ldict[reducer](None, k1, v1):
                                    print(k2, v2) 
                            self.master_node_conn.sendall(struct.pack('>I', REDUCE_CONFIRM))
                        
                if (mask & selectors.EVENT_WRITE) and data.outb: 
                    try: 
                        sock.sendall(data.outb)
                        data.outb = b""
                    except OSError: 
                        continue 


    def run(self): 
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.bind((self.host, self.port))
        lsock.listen() 
        lsock.setblocking(False) 
        self.lsock = lsock 

        print("Worker node at ", self.port)
        self.sel = selectors.DefaultSelector() 
        self.workers_sel = selectors.DefaultSelector() 
        self.sel.register(lsock, selectors.EVENT_READ, data=None) 

        self.mapreduce_queue = queue.Queue() 
        self.heartbeat_queue = queue.Queue() 
        self.write_to_master_node_queue = queue.Queue()

        threading.Thread(target=self.mapreduce_thread).start()
        threading.Thread(target=self.heartbeat_thread).start()
        threading.Thread(target=self.communication_with_workers).start()
        
        while True: 
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.data is None: self.accept_wrapper()
                else: self.service_connection_with_master_node(key, mask)
    
    def service_connection_with_master_node(self, key, mask):
        sock, data = key.fileobj, key.data 
        if mask & selectors.EVENT_READ:  
            raw_opcode = self._recvall(sock, 4)
            if not raw_opcode: 
                self.sel.unregister(sock)
                sock.close() 
                return 
            opcode = struct.unpack('>I', raw_opcode)[0]
            if opcode == HEARTBEAT: 
                self.heartbeat_queue.put(HEARTBEAT)
            elif opcode == MAP: 
                mapper_, input_ = self._recv_n_args(sock, 1, True)
                self.mapreduce_queue.put((opcode, mapper_, input_))
            elif opcode == REDUCE: 
                reducer_, worker_locs = self._recv_n_args(sock, 1, True)
                self.mapreduce_queue.put((opcode, reducer_, worker_locs))
            elif opcode == UPDATE: 
                locs = self._recv_n_args(sock, 0, True)
                self.mapreduce_queue.put((opcode, locs))
            
        if mask & selectors.EVENT_WRITE: 
            while not self.write_to_master_node_queue.empty(): 
                msg = self.write_to_master_node_queue.get() 
                sock.sendall(msg)
    
    def heartbeat_thread(self): 
        while True: 
            if not self.heartbeat_queue.empty(): 
                heartbeat_msg = self.heartbeat_queue.get()
                outb = struct.pack('>I', HEARTBEAT_CONFIRM) 
                self.write_to_master_node_queue.put(outb)
    
    def mapreduce_thread(self): 
        while True: 
            msg = self.mapreduce_queue.get() 
            opcode = msg[0]
            if opcode == MAP:
                self.state = {}
                mapper_, input_ = msg[1], msg[2]
                ldict = {}
                exec(mapper_, globals(), ldict)
                mapper = "mapper"
                for k1, v1 in input_: 
                    for k2, v2 in ldict[mapper](None, k1, v1):
                        """ hard coding 2 right now """
                        hash_ = len(k2) % 3
                        if hash_ not in self.state: 
                            self.state[hash_] = []
                        self.state[hash_].append((k2, v2))
                to_send = self._pack_n_args(MAP_CONFIRM, [])
                self.write_to_master_node_queue.put(to_send)
            elif opcode == REDUCE: 
                if self.kill_process: 
                    pid = getpid()
                    kill(pid, SIGKILL)
                self.reducer, self.worker_locs = msg[1], msg[2]
                # run communication_with_workers thread 
                for (host, port) in self.worker_locs: 
                    try: 
                        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        temp.connect((host, port))
                        self.receive_state[(host, port)] = [temp, None]
                    except (ConnectionRefusedError, TimeoutError): 
                        self.receive_state[(host, port)] = [None, None]
                        print("Connection was refused from ", (host, port))
                        continue 
                time.sleep(1)

                for worker_socket in [self.receive_state[k][0] for k in self.receive_state.keys()]: 
                    if worker_socket != None: 
                        outb = struct.pack('>I', REQUEST) + struct.pack('>I', self.hash)
                        data = types.SimpleNamespace(outb=outb)
                        try: 
                            self.workers_sel.register(worker_socket, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)
                        except (OSError, ValueError): 
                            continue 

            elif opcode == UPDATE: 
                old, new = msg[1][0]
                if old in self.receive_state and self.receive_state[old][1] == None: 
                    # old_socket = self.receive_state[old]
                    # self.workers_sel.unregister(old_socket)
                    # old_socket.close() 
                    del self.receive_state[old]
                    try:
                        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        temp.connect((new))
                        self.receive_state[new] = [temp, None]
                    except (ConnectionRefusedError, TimeoutError): 
                        continue 
                    outb = struct.pack('>I', REQUEST) + struct.pack('>I', self.hash)
                    data = types.SimpleNamespace(outb=outb)
                    self.workers_sel.register(temp, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)
                   

    def _recvall(self, sock, n):
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

    def _recv_n_args(self, sock, n, pickled=None): 
        args = []
        for _ in range(n): 
            arg_len = struct.unpack('>I', self._recvall(sock, 4))[0]
            args.append(self._recvall(sock, arg_len).decode("utf-8", "strict"))
        if pickled: 
            pickle_len = struct.unpack('>I', self._recvall(sock, 4))[0]
            raw_pickle = self._recvall(sock, pickle_len)
            pickled_obj = pickle.loads(raw_pickle)
            args.append(pickled_obj)
        return args
    
    def _pack_n_args(self, opcode, reg_args, pickled=None): 
        to_send = struct.pack('>I', opcode)
        for arg in reg_args: 
            to_send += struct.pack('>I', len(arg)) + arg.encode("utf-8")
        if pickled: 
            to_send += struct.pack('>I', len(pickled)) + pickled
        return to_send 
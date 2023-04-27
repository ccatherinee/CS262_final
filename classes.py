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

class MRJob: 
    def __init__(self, n=2): 
        self.master_node = MasterNode(n) 
    
    def mapper(self, key, value): 
        yield key, value 
    
    def reducer(self, key, value): 
        yield key, value 
    
    def run(self, inputs): 
        self.master_node.run(inputs, self.mapper, self.reducer)
    
class MasterNode: 
    def __init__(self, n): 
        # selector which monitors connections to worker nodes 
        self.sel = selectors.DefaultSelector()

        # maps host, port to (input, hash, data.outb, sock)
        self.worker_info = {}
        self.heartbeat_queue = queue.Queue()
        self.worker_confirmations_queue = queue.Queue()

        threading.Thread(target=self.monitor_workers_thread).start() 

        # spawn up n servers
        for offset in range(n): 
            port = WORKER_PORT_START + offset 
            worker_process = Process(target=Worker("", port, offset).run, args=())
            worker_process.start() 

            time.sleep(1)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("", port))
            print("Master node connecting to ", port)

            data = types.SimpleNamespace(outb=[])
            self.sel.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)
            
            self.worker_info[("", port)] = [None, offset, data.outb, sock]

    def split(self, a, n): 
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
    
    def mapper_setup(self, inputs, mapper): 
        # splitting up the inputs into several chunks and putting it in self.worker_info
        idx = 0 
        inputs = list(self.split(inputs, len(self.worker_info)))
        for k in self.worker_info.keys(): 
            self.worker_info[k][0] = inputs[idx]
            idx += 1
        for k in self.worker_info.keys(): 
            # pack opcode MAP, pickled input_, mapper function 
            input_ = self.worker_info[k][0]
            mapper_str = getsource(mapper) 
            mapper_str = mapper_str.strip() 
            to_send = self._pack_n_args(MAP, [mapper_str], pickle.dumps(input_))
            self.worker_info[k][2].append(to_send)
        self.wait_until_confirmation()

    def reducer_setup(self, reducer): 
        for k in self.worker_info.keys(): 
            reducer_str = getsource(reducer)
            reducer_str = reducer_str.strip() 
            worker_locs = list(self.worker_info.keys())
            to_send = self._pack_n_args(REDUCE, [reducer_str], pickle.dumps(worker_locs))
            self.worker_info[k][2].append(to_send)
        self.wait_until_confirmation()
    
    # def heartbeat_thread()
    def monitor_workers_thread(self): 
        while True: 
            events = self.sel.select(timeout=None) 
            for key, mask in events: 
                sock, data = key.fileobj, key.data
                if mask & selectors.EVENT_READ:
                    raw_opcode = self._recvall(sock, 4)
                    if not raw_opcode: return 
                    opcode = struct.unpack('>I', raw_opcode)[0]
                    loc = sock.getpeername()
                    if opcode == HEARTBEAT_CONFIRM: 
                        self.heartbeat_queue.put(loc)
                    else: 
                        self.worker_confirmations_queue.put(loc)
                if (mask & selectors.EVENT_WRITE) and len(data.outb) > 0:
                    while len(data.outb) > 0: 
                        temp = data.outb.pop()
                        sock.sendall(temp)

    def wait_until_confirmation(self): 
        unconfirmed = list(self.worker_info.keys())
        while len(unconfirmed) > 0: 
            loc = self.worker_confirmations_queue.get() 
            if loc[0] == '127.0.0.1': 
                loc = ('', loc[1])
            if loc in unconfirmed: 
                unconfirmed.remove(loc)

    def run(self, inputs, mapper, reducer): 
        self.mapper_setup(inputs, mapper) 
        self.reducer_setup(reducer) 
        # return it back to client somehow

    def _recvall(self, sock, n):
        data = bytearray() 
        while len(data) < n: 
            packet = sock.recv(n - len(data))
            if not packet:
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
    def __init__(self, host, port, hash_): 
        # host and port the worker is at 
        self.host, self.port = host, port 
        # the hash this worker is responsible for 
        self.hash = hash_ 
        # maps hashes to list of (k, v)
        self.state = None 
        # maps (host, port) to the list of (k, v) you've received from that (host, port) 
        self.receive_state = {}
        self.reduce_state = {}
    
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
                    if not raw_opcode: return 
                    opcode = struct.unpack('>I', raw_opcode)[0]

                    if opcode == REQUEST: 
                        raw_hash = self._recvall(sock, 4)
                        hash_ = struct.unpack('>I', raw_hash)[0]
                        if not hash_ in self.state: 
                            self.state[hash_] = []
                        data.outb += self._pack_n_args(GIVE, [], pickle.dumps(self.state[hash_]))

                    elif opcode == GIVE: 
                        keys_and_values = self._recv_n_args(sock, 0, True)[0]
                        self.receive_state[sock.getpeername()] = keys_and_values 
                        if len(self.receive_state) == len(self.worker_locs): 
                            for _, list_ in self.receive_state.items(): 
                                for k, v in list_: 
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
                            
                elif (mask & selectors.EVENT_WRITE) and data.outb: 
                    sock.sendall(data.outb)
                    data.outb = b""

    def run(self): 
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.bind((self.host, self.port))
        lsock.listen() 
        lsock.setblocking(False) 
        self.lsock = lsock 

        print("Listening at ", self.port)
        self.sel = selectors.DefaultSelector() 
        self.workers_sel = selectors.DefaultSelector() 
        self.sel.register(lsock, selectors.EVENT_READ, data=None) 

        self.mapreduce_queue = queue.Queue() 
        self.heartbeat_queue = queue.Queue() 
        self.write_to_master_node_queue = queue.Queue()

        threading.Thread(target=self.mapreduce_thread).start()
        # threading.Thread(target=self.heartbeat_thread).start()
        
        while True: 
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.data is None: self.accept_wrapper()
                else: self.service_connection_with_master_node(key, mask)
    
    def service_connection_with_master_node(self, key, mask):
        sock, data = key.fileobj, key.data 
        if mask & selectors.EVENT_READ:  
            raw_opcode = self._recvall(sock, 4)
            if not raw_opcode: return 
            opcode = struct.unpack('>I', raw_opcode)[0]
            if opcode == HEARTBEAT: 
                self.heartbeat_queue.put(HEARTBEAT)
            elif opcode == MAP: 
                mapper_, input_ = self._recv_n_args(sock, 1, True)
                self.mapreduce_queue.put((opcode, mapper_, input_))
            elif opcode == REDUCE: 
                reducer_, worker_locs = self._recv_n_args(sock, 1, True)
                self.mapreduce_queue.put((opcode, reducer_, worker_locs))
        if mask & selectors.EVENT_WRITE: 
            while not self.write_to_master_node_queue.empty(): 
                msg = self.write_to_master_node_queue.get() 
                sock.sendall(msg)

    def heartbeat_thread(self): 
        while not self.heartbeat_queue.empty(): 
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
                        hash_ = len(k2) % 2
                        if hash_ not in self.state: 
                            self.state[hash_] = []
                        self.state[hash_].append((k2, v2))
                to_send = self._pack_n_args(MAP_CONFIRM, [])
                self.write_to_master_node_queue.put(to_send)
            elif opcode == REDUCE: 
                self.reducer, self.worker_locs = msg[1], msg[2]
                # run communication_with_workers thread 
                threading.Thread(target=self.communication_with_workers).start()
                for (host, port) in self.worker_locs: 
                    try: 
                        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        temp.connect((host, port))
                    except (ConnectionRefusedError, TimeoutError): 
                        continue 
                    outb = struct.pack('>I', REQUEST) + struct.pack('>I', self.hash)
                    data = types.SimpleNamespace(outb=outb)
                    self.workers_sel.register(temp, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)
    
    def _recvall(self, sock, n):
        data = bytearray() 
        while len(data) < n: 
            packet = sock.recv(n - len(data))
            if not packet:
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
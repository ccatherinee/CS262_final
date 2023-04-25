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

class MRJob: 
    def __init__(self, n=2): 
        # create an instance of LoadBalancer 
        self.load_balancer = LoadBalancer(n)

    def mapper(self, key, value): 
        raise NotImplementedError

    def reducer(self, key, value): 
        raise NotImplementedError

    def run(self, inputs): 
        self.load_balancer.run(inputs, self.mapper, self.reducer) 

class LoadBalancer: 
    def __init__(self, n): 
        # selector which monitors connections to worker nodes
        self.sel = selectors.DefaultSelector()
        # spawn up n servers 
        self.worker_states = {}
        self.worker_locs = []
        for port in range(WORKER_PORT_START, WORKER_PORT_START + n): 
            worker_process1 = Process(target=Worker().communication_with_lb, args=("",port,))
            worker_process1.start() 
            time.sleep(1)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("", port))
            print("lb connecting to ", port)
            self.sel.register(sock, selectors.EVENT_READ)

            self.worker_states[sock] = {}
            self.worker_locs.append(("", port))

    def split(self, a, n):
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

    def mapper_setup(self, inputs, mapper): 
        idx = 0 
        inputs = list(self.split(inputs, len(self.worker_states)))
        for sock in self.worker_states.keys(): 
            self.worker_states[sock] = inputs[idx]
            idx += 1
        for sock, input_ in self.worker_states.items(): 
            # pack the opcode MAP, pack pickled input_, pack mapper function 
            mapper_str = getsource(mapper)
            mapper_str = mapper_str.strip() 
            to_send = self._pack_n_args(MAP, [mapper_str], pickle.dumps(input_))
            sock.sendall(to_send)
        time.sleep(2)

    def shuffle_setup(self): 
        """
        def hash_func(key, worker_locs): 
            n = len(worker_locs)
            return worker_locs[hash(key) % n] """
        # send it over to the worker nodes 
        for sock in self.worker_states.keys(): 
            to_send = self._pack_n_args(SHUFFLE, [], pickle.dumps(self.worker_locs))
            sock.sendall(to_send)
        time.sleep(2)

    def reducer_setup(self, reducer): 
        # send op code and reducer over to the nodes in self.worker_states
        for sock in self.worker_states.keys(): 
            reducer_str = getsource(reducer)
            reducer_str = reducer_str.strip()
            to_send = self._pack_n_args(REDUCE, [reducer_str])
            sock.sendall(to_send)
        time.sleep(2)
    
    def wait_until_confirmation(self, opcode): 
        unconfirmed = list(self.worker_states.keys())
        t_end = time.time() + 20                         
        while len(unconfirmed) > 0: 
            if time.time() < t_end: 
                events = self.sel.select(timeout=-1)
                for key, _ in events: 
                    sock = key.fileobj
                    if sock in unconfirmed: 
                        temp = self._recvall(sock, 4)
                        unconfirmed.remove(sock)
                        print(temp, sock.getsockname())

    def run(self, inputs, mapper, reducer): 
        self.mapper_setup(inputs, mapper) 
        self.shuffle_setup() 
        self.reducer_setup(reducer)
        # send back to client 
    
    def _pack_n_args(self, opcode, args, pickled=None): 
        to_send = struct.pack('>I', opcode)
        for arg in args: 
            to_send += struct.pack('>I', len(arg)) + arg.encode("utf-8")
        if pickled: 
            to_send += struct.pack('>I', len(pickled)) + pickled
        return to_send 
    
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

class Worker: 
    def __init__(self): 
        pass

    def accept_wrapper(self): 
        # register the selector for loadbalancer as read only and register the selector for the other workers as read and write
        conn, addr = self.lsock.accept()
        conn.setblocking(False)
        if self.state == None: 
            data = types.SimpleNamespace(addr=addr)
            self.sel.register(conn, selectors.EVENT_READ, data=data)
            print("Accepting the lb")
        else: 
            data = types.SimpleNamespace(addr=addr, outb=b"")
            self.workers_sel.register(conn, selectors.EVENT_READ, data=data)

    def communication_with_workers(self): 
        while True: 
            events = self.workers_sel.select(timeout=None)
            for key, mask in events: 
                sock, data = key.fileobj, key.data
                if data is None: 
                    self.accept_wrapper() 
                elif mask & selectors.EVENT_READ: 
                    # stupid fathead
                    for k, v in self._recv_n_args(sock, 0, True)[0]: 
                        if k not in self.state: 
                            self.state[k] = []
                        self.state[k] += v 
                elif mask & selectors.EVENT_WRITE and data.outb:
                    sock.sendall(data.outb)
                    data.outb = b""

    def communication_with_lb(self, host, port): 
        # create a listening socket 
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.bind((host, port))
        lsock.listen()
        lsock.setblocking(False)
        self.lsock = lsock 

        print("listening at ", port)
        # register 
        self.sel = selectors.DefaultSelector()
        self.workers_sel = selectors.DefaultSelector()
        self.sel.register(lsock, selectors.EVENT_READ, data=None)

        # run communication_with_workers thread 
        threading.Thread(target=self.communication_with_workers).start()
        self.state = None
        # iterate through it, put in {port: (k, v)}
        self.shuffle_state = {}
        self.shuffling_state = {}
        
        while True: 
            events = self.sel.select(timeout=None)
            for key, mask in events: 
                if key.data is None: 
                    self.accept_wrapper()
                else: 
                    self.service_connection_with_lb(key, mask)

    def service_connection_with_lb(self, key, mask): 
        sock, data = key.fileobj, key.data
        if mask & selectors.EVENT_READ: 
            raw_opcode = self._recvall(sock, 4)
            if not raw_opcode: return 
            opcode = struct.unpack('>I', raw_opcode)[0]
            if opcode == MAP: 
                self.state = {}
                mapper_, input_ = self._recv_n_args(sock, 1, True)
                ldict = {}
                exec(mapper_, globals(), ldict)
                mapper = "mapper"
                for k1, v1 in input_: 
                    for k2, v2 in ldict[mapper](None, k1, v1):
                        if not k2 in self.state: self.state[k2] = []
                        self.state[k2].append(v2)
                to_send = self._pack_n_args(MAP_CONFIRM, [], pickle.dumps(self.state))
                sock.sendall(to_send)
                # {"the": [1, 1]}
            elif opcode == SHUFFLE: 
                worker_locs = self._recv_n_args(sock, 0, True)[0]
                for k, v in self.state.items(): 
                    self.shuffle_state[k] = v
                self.state = {}
                for k, v in self.shuffle_state.items(): 
                    n = len(worker_locs)
                    loc = worker_locs[len(k) % n] 
                    # loc = hash_func[k]
                    if loc not in self.shuffling_state: 
                        self.shuffling_state[loc] = ([], False)
                    self.shuffling_state[loc][0].append((k, v))
                self.shuffle_state = {}

                # info consists of ([(k, [1,1]) (k, [1])], BOOL)
                for loc, info in self.shuffling_state.items(): 
                    if info[1] == False: 
                        # create a new socket, register it in self.worker_sel for write events and put info[0] into data.outb
                        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        temp.connect((loc[0], loc[1]))
                        outb = self._pack_n_args(None, [], pickle.dumps(info[0]))
                        data = types.SimpleNamespace(outb=outb)
                        self.workers_sel.register(temp, selectors.EVENT_WRITE, data=data)
                to_send = self._pack_n_args(SHUFFLE_CONFIRM, [])
                sock.sendall(to_send)
            elif opcode == REDUCE: 
                print(self.state, "hot air balloon")
                reducer_ = self._recv_n_args(sock, 1)[0]
                ldict = {}
                exec(reducer_, globals(), ldict)
                reducer = "reducer"
                for k, v in self.state.items(): 
                    for k2, v2 in ldict[reducer](None, k, v): 
                        self.state[k2] = v2
                print(self.state, "permanent")
                to_send = self._pack_n_args(REDUCE_CONFIRM, [], pickle.dumps(self.state))
                sock.sendall(to_send)
                """
                FAILURE HANDLING: 
                send stuff to the correct place 
                if can't send, send a FAILURE to LB  
                else succeeds, then send a CONFIRMATION 
                """
            """ elif opcode = SHUFFLE_UPDATE: 
                get broken, new port and re-send """
            """
            elif opcode = SHUFFLE_STATE_REQUEST: 
                send over self.state """

    def _pack_n_args(self, opcode, args, pickled=None): 
        to_send = b""
        if opcode != None: 
            to_send = struct.pack('>I', opcode)
        for arg in args: 
            to_send += struct.pack('>I', len(arg)) + arg.encode("utf-8")
        if pickled != None: 
            to_send += struct.pack('>I', len(pickled)) + pickled
        return to_send 

    def _recvall(self, sock, n): 
        data = bytearray()
        while len(data) < n: 
            packet = sock.recv(n - len(data))
            if not packet: 
                return None 
            data.extend(packet) 
        return data 
    
    def _recv_n_args(self, sock, n, pickled=False): 
        args = []
        for _ in range(n): 
            arg_len = struct.unpack('>I', self._recvall(sock, 4))[0]
            args.append(self._recvall(sock, arg_len).decode("utf-8", "strict"))
        if pickled == True: 
            pickle_len = struct.unpack('>I', self._recvall(sock, 4))[0] 
            raw_pickle = self._recvall(sock, pickle_len)
            pickled_obj = pickle.loads(raw_pickle)
            args.append(pickled_obj)
        return args
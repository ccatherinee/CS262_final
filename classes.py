import time 
from constants import * 
import pickle 
from dill.source import getsource 

class MRJob: 
    def __init__(self, n=5): 
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
            worker = Worker("", port)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # may need to sleep here 
            sock.connect(("", port))
            self.sel.register(sock, selectors.EVENT_READ)

            self.worker_states[sock] = {}
            self.worker_ports.append(("", port))

    def split(self, a, n):
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

    def mapper_setup(self, inputs, mapper): 
        idx = 0 
        inputs = list(self.split(inputs, len(self.worker_states)))
        for sock in self.worker_states.keys(): 
            self.worker_states[sock] = inputs[idx]
            idx += 1
        for sock, input_ in self.worker_states: 
            # pack the opcode MAP, pack pickled input_, pack mapper function 
            to_send = self._pack_n_args(MAP, pickle.dumps(input_), getsource(mapper))
            sock.sendall(to_send)
        self.wait_until_confirmation(MAP)

    def shuffle_setup(self): 
        def hash_func(key, worker_ports): 
            n = len(worker_ports)
            return worker_ports[hash(key) % n]
        # send it over to the worker nodes 
        for sock in self.worker_states.keys(): 
            to_send = self.pack_n_args(SHUFFLE, pickle.dumps(self.worker_ports), getsource(hash_func))
            sock.sendall(to_send)
        self.wait_until_confirmation(SHUFFLE)

    def reducer_setup(self, reducer): 
        # send op code and reducer over to the nodes in self.worker_states
        for sock in self.worker_states.keys(): 
            to_send = self._pack_n_args(REDUCE, getsource(reducer))
            sock.sendall(to_send)
        self.wait_until_confirmation(REDUCE)
    
    def wait_until_confirmation(self, opcode): 
        unconfirmed = self.worker_states.keys() 
        t_end = time.time() + 10                         
        while len(unconfirmed) > 0: 
            if time.time() < t_end: 
                events = self.sel.select(timeout=-1)
                for key, _ in events: 
                    sock = key.fileobj
                    if sock in unconfirmed: 
                        temp = self._recvall(sock, 4)
                        not_responded.remove(sock)
                        print(temp, sock.getsockname())

    def run(self, inputs, mapper, reducer): 
        self.mapper_setup(inputs, mapper) 
        self.shuffle_setup() 
        self.reducer_setup(reducer)
        # send back to client 
    
    def _pack_n_args(self, opcode, args): 
        to_send = struct.pack('>I', opcode)
        for arg in args: 
            to_send += struct.pack('>I', len(arg)) + arg.encode("utf-8")
        return to_send 
    
    def _recvall(self, sock, n):
        data = bytearray() 
        while len(data) < n: 
            packet = sock.recv(n - len(data))
            if not packet:
                return None 
            data.extend(packet)
        return data 

    def _recv_n_args(self, sock, n): 
        args = []
        for _ in range(n): 
            arg_len = struct.unpack('>I', self._recvall(sock, 4))[0]
            args.append(self._recvall(sock, arg_len).decode("utf-8", "strict"))
        return args 

class Worker: 
    def __init__(self, host, port): 
        # create a listening socket 
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.bind((host, port))
        lsock.listen()
        lsock.setblocking(False)
        self.lsock = lsock 

        # register 
        self.sel = selectors.DefaultSelector
        self.workers_sel = selectors.DefaultSelector
        self.sel.register(lsock, selectors.EVENT_READ, data=None)

        self.state = None
        # iterate through it, put in {port: (k, v)}
        self.shuffle_state = {}
        self.shuffling_state = {}

    def accept_wrapper(self): 
        # register the selector for loadbalancer as read only and register the selector for the other workers as read and write
        conn, addr = self.sock.accept()
        conn.setblocking(False)
        if self.state == None: 
            data = types.SimpleNamespace(addr=addr)
            self.sel.register(conn, selectors.EVENT_READ, data=data)
        else: 
            data = types.SimpleNamespace(addr=addr, outb=b"")
            self.workers_sel.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)

    def communication_with_workers(self): 
        while True: 
            events = self.workers_sel.select(timeout=None)
            for key, mask in events: 
                sock, data = key.fileobj, key.data
                if data is None: 
                    self.accept_wrapper() 
                elif mask & selectors.EVENT_READ: 
                    for k, v in self._recv_n_args(sock, 1): 
                        if k not in self.state: 
                            self.state[k] = []
                        self.state[k] += v 
                elif mask & selectors.EVENT_WRITE and data.outb: 
                    sock.sendall(data.outb)
                    data.outb = b""

    def communication_with_lb(self): 
        while True: 
            events = self.sel.select(timeout=None)
            for key, mask in events: 
                if key.data is None: 
                    self.accept_wrapper()
                else: 
                    self.service_connection_with_lb(key, mas)

    def service_connection_with_lb(self, key, mask): 
        sock, data = key.fileobj
        if mask & selectors.EVENT_READ: 
            raw_opcode = self._recvall(sock, 4)
            if not raw_opcode: return 
            opcode = struct.unpack('>I', raw_opcode)[0]
            if opcode == MAP: 
                input_, mapper = self._recv_n_args(sock, 2)
                exec mapper 
                for k1, v1 in input_: 
                    for k2, v2 in mapper(k1, v1):
                        if not k2 in self.state: self.state[k2] = []
                        self.state[k2].append(v2)
                to_send = self._pack_n_args(MAP_CONFIRM, pickle.dumps(self.state))
                sock.sendall(to_send)
                # {"the": [1, 1]}
            elif opcode == SHUFFLE: 
                worker_ports, hash_func = self.recv_n_args(sock, 2)
                for k, v in self.state: 
                    self.shuffle_state[k] = v
                self.state = {}
                for k, v in self.shuffle_state: 
                    loc = hash_func[k]
                    if loc not in self.shuffling_state: 
                        self.shuffling_state[loc] = ([], False)
                    self.shuffling_state[loc][0].append((k, v))
                self.shuffle_state = {}
                # info consists of ([(k, [1,1]) (k, [1])], BOOL)
                for loc, info in self.shuffling_state: 
                    if info[0] == False: 
                        # create a new socket, register it in self.worker_sel for write events and put info[0] into data.outb
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((loc[0], loc[1]))
                        outb = self._pack_n_args(info[1])
                        data = types.SimpleNamespace(addr=addr, outb=outb)
                        self.workers_sel.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data=data)
                to_send = self._pack_n_args(SHUFFLE_CONFIRM)
                sock.sendall(to_send)
            elif opcode == REDUCE: 
                reducer = self.self._recv_n_args(sock, 1)
                exec reducer
                for k, v in self.state: 
                    self.state[k] = reducer(v)
                to_send = self._pack_n_args(REDUCE_CONFIRM, pickle.dumps(self.state))
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

    def _pack_n_args(self, opcode, args): 
        to_send = struct.pack('>I', opcode)
        for arg in args: 
            to_send += struct.pack('>I', len(arg)) + arg.encode("utf-8")
        return to_send 

    def _recvall(self, sock, n): 
        data = bytearray()
        while len(data) < n: 
            packet = sock.recv(n - len(data))
            if not packet: 
                return None 
            data.extend(packet) 
        return data 
    
    def _recv_n_args(self, sock, n): 
        args = []
        for _ in range(n): 
            arg_len = struct.unpack('>I', self._recvall(sock, 4))[0]
            args.append(self._recvall(sock, arg_len).decode("utf-8", "strict"))
        return args 
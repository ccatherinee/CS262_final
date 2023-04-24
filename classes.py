import time 
from constants import * 

class MRJob: 
    def __init__(self, n=5): 
        # create a client socket 
        # create an instance of LoadBalancer 
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
        self.worker_inputs = {}
        for port in range(WORKER_PORT_START, WORKER_PORT_START + n): 
            worker = Worker("", port)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.worker_inputs[sock] = None
        for sock in self.worker_inputs.keys(): 
            sock.connect(("", port))
            self.sel.register(sock, selectors.EVENT_READ)

    def mapper_setup(self, inputs, mapper): 
        # split up inputs and put them in self.worker_inputs 
        for sock, input_ in self.worker_inputs: 
            to_send = self._pack_n_args(MAP, input_)
            sock.sendall(to_send)
        self.wait_until_confirmation 

    def shuffle_setup(self): 
        # create hash function 
        # send it over to the worker nodes 
        self.wait_until_confirmation

    def reducer_setup(self, reducer): 
        # send op code and reducer over to the nodes in self.worker_inputs
        self.wait_until_confirmation

    def wait_until_confirmation(self): 
        unconfirmed = self.worker_inputs.keys() 
        t_end = time.time() + 10 
        while len(unconfirmed) > 0: 
            if time.time() < t_end: 
                events = self.sel.select(timeout=-1)
                for key, _ in events: 
                    sock = key.fileobj 
                    temp = self._recvall(sock, 4)
                    if temp: 
                        unconfirmed.remove(sock)
                    
                # check selector, remove that port from the unconfirmed, and read the entire state and update the dictionary 
            else: # iterate through unconfirmed, spin up new worker, update self.worker_inputs and unconfirmed, reset the timer 

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

class Worker: 
    def __init__(self, host, port): 
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((HOST, PORT))
        sock.listen()
        self.sock = sock
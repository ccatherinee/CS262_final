import unittest
from unittest import mock 
from unittest.mock import patch, call, ANY
import socket 
import selectors 
import user 
import master
import worker
import constants 
import struct 
import queue 
import types 
import pathlib

class TestWorker(unittest.TestCase):

    @mock.patch("builtins.open")
    @mock.patch("json.loads")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_reduce(self, mock_selector, mock_socket, mock_loads, mock_open):
        self.worker_node = worker.Worker(testing=True)
        self.assertEqual(self.worker_node.write_to_master_queue.qsize(), 1)
        self.worker_node.M = 2
        self.worker_node.R = 2
        self.worker_node.reduce_task = 2
        self.worker_node.request_intermediate_from = queue.Queue()
        self.worker_node.request_intermediate_from.put((1,'host1',1234))
        self.worker_node.request_intermediate_from.put((2,'host2',4321))

        self.worker_node.intermediate_results = queue.Queue()
        self.worker_node.intermediate_results.put((1,{}))
        self.worker_node.intermediate_results.put((2,{}))

        def f(a,b,c): yield sum(c)
        self.worker_node.reducer = f 
        mock_loads.side_effect = [[('hello',1), ('my',1),('name',1),('is',1),('hello',1)], [('yes',1), ('my',1), ('name',1),('is',1)]]
        self.worker_node.reduce_thread()

        mock_open.assert_called_once_with('mr-output-2.txt','w')
        mock_open().__enter__().write.assert_has_calls([call("hello\t2\n"), call("my\t2\n"),call("name\t2\n"),call("is\t2\n"),call("yes\t1\n")])
        self.assertEqual(self.worker_node.request_intermediate_from.empty(), True)
        mock_socket.return_value.connect.assert_has_calls([call(('host1',1234)), call(('host2',4321))])
        mock_selector.return_value.register.assert_has_calls([call(ANY,1,data=None), call(ANY,2,data=ANY)])
        self.assertEqual(self.worker_node.intermediate_results.empty(), True)
        self.assertEqual(self.worker_node.write_to_master_queue.qsize(), 3)
        self.assertEqual(self.worker_node.reduce_task, None)

    @mock.patch("builtins.hash")
    @mock.patch("json.dump")
    @mock.patch("builtins.open")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_map(self, mock_sel, mock_sock, mock_open, mock_dump, mock_hash):
        self.worker_node = worker.Worker(testing=True)
        self.assertEqual(self.worker_node.write_to_master_queue.qsize(), 1)
        self.worker_node.M = 2
        self.worker_node.R = 2
        self.worker_node.map_task = 2

        def f(a,b,c):
            import string
            line = c.translate(str.maketrans('', '', string.punctuation))
            for word in line.split():
                yield word.lower(), 1

        mock_hash.side_effect = [1,2,2,1,2,1,1,2,2]
                
        self.worker_node.mapper = f 
        self.worker_node.map_thread("hello my name is\n yes that is my name")
        mock_hash.assert_has_calls([call('hello'), call('my'), call('name'), call('is'), call('yes'), call('that'), call('is'), call('my'), call('name')], any_order=True)
        mock_open.assert_has_calls([call('mr-2-1.json','w'), call('mr-2-2.json','w')], any_order=True)
        mock_dump.assert_has_calls([call([('my', 1), ('name', 1), ('yes', 1), ('my', 1), ('name', 1)], ANY), call([('hello', 1), ('is', 1), ('that', 1), ('is', 1)], ANY)])
        self.assertEqual(self.worker_node.write_to_master_queue.qsize(),3)
        self.assertEqual(self.worker_node.map_task, None)

    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_accept_worker_connection(self, mock_sel, mock_sock):
        self.worker_node = worker.Worker(testing=True)
        mock_conn = mock.Mock(name="conn")
        mock_sock.return_value.accept.return_value = (mock_conn, ("hostasdf",1234))
        self.worker_node.accept_worker_connection()
        mock_sel.return_value.register.assert_has_calls([call(mock_conn, selectors.EVENT_READ, data=ANY), call(mock_conn, selectors.EVENT_WRITE,data=ANY)])

    @mock.patch("worker.Worker._recvall")
    @mock.patch("struct.unpack")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_master_complete(self, mock_sel, mock_sock, mock_unpack, mock_recvall):
        self.worker_node = worker.Worker(testing=True)
        mock_key = mock.Mock(name="key")

        mock_unpack.return_value = (constants.ALL_TASKS_COMPLETE,)
        status = self.worker_node.service_master_connection(mock_key, selectors.EVENT_READ)
        mock_sel.return_value.unregister.assert_called_with(self.worker_node.master_sock)
        self.worker_node.master_sock.close.assert_called_once()
        self.assertEqual(status, True)
        
    @mock.patch("worker.Worker._recvall")
    @mock.patch("struct.unpack")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_master_down(self, mock_sel, mock_sock, mock_unpack, mock_recvall):
        self.worker_node = worker.Worker(testing=True)
        mock_key = mock.Mock(name="key")
        
        #master not up, so worker should abort
        mock_recvall.return_value = None
        status = self.worker_node.service_master_connection(mock_key, selectors.EVENT_READ)
        mock_sel.return_value.unregister.assert_called_with(self.worker_node.master_sock)
        self.worker_node.master_sock.close.assert_called_once()
        self.assertEqual(status, True)

    @mock.patch("time.sleep")
    @mock.patch("worker.Worker._recvall")
    @mock.patch("struct.unpack")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_master_no_avail(self, mock_sel, mock_sock, mock_unpack, mock_recvall, mock_sleep):
        self.worker_node = worker.Worker(testing=True)
        mock_key = mock.Mock(name="key")
        mock_unpack.return_value = (constants.NO_AVAILABLE_TASK,)
        start_bytes = self.worker_node.bytes_sent
        status = self.worker_node.service_master_connection(mock_key, selectors.EVENT_READ)
        mock_sleep.assert_called_once_with(1)
        self.worker_node.master_sock.sendall.assert_called_once_with(struct.pack('>Q',constants.REQUEST_TASK))
        self.assertEqual(self.worker_node.bytes_sent - start_bytes, 8)
        self.assertEqual(status, False)

    @mock.patch("threading.Thread")
    @mock.patch("worker.Worker._recvall")
    @mock.patch("struct.unpack")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_master_map(self, mock_sel, mock_sock, mock_unpack, mock_recvall, mock_thread):
        self.worker_node = worker.Worker(testing=True)
        mock_key = mock.Mock(name="key")
        mock_unpack.side_effect = [(constants.MAP_TASK,), (2,), (2,), (2,), (100,), (1000,)]
        mock_recvall.return_value.decode.side_effect = ["def mapper(a,b,c): return b", "hello this is the input"]
        status = self.worker_node.service_master_connection(mock_key, selectors.EVENT_READ)
        self.assertEqual(self.worker_node.map_task, 2)
        self.assertEqual(self.worker_node.M, 2)
        self.assertEqual(self.worker_node.R, 2)
        self.assertEqual(self.worker_node.mapper(1,2,3),2)
        mock_thread.assert_called_once_with(target=self.worker_node.map_thread, args=("hello this is the input",))
        mock_thread.return_value.start.assert_called_once()
        self.assertEqual(status, False)

    @mock.patch("threading.Thread")
    @mock.patch("worker.Worker._recvall")
    @mock.patch("struct.unpack")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_master_reduce(self, mock_sel, mock_sock, mock_unpack, mock_recvall, mock_thread):
        self.worker_node = worker.Worker(testing=True)
        mock_key = mock.Mock(name="key")
        mock_unpack.side_effect = [(constants.REDUCE_TASK,), (1,), (2,), (2,), (100,), (1000,)]
        mock_recvall.return_value.decode.return_value = "def reducer(a,b,c): return sum(c)"
        status = self.worker_node.service_master_connection(mock_key, selectors.EVENT_READ)
        self.assertEqual(self.worker_node.reduce_task, 1)
        self.assertEqual(self.worker_node.M, 2)
        self.assertEqual(self.worker_node.R, 2)
        self.assertEqual(self.worker_node.reducer(1,2,[1,1,1,1,2]),6)
        mock_thread.assert_called_once_with(target=self.worker_node.reduce_thread)
        mock_thread.return_value.start.assert_called_once()
        self.assertEqual(status, False)
        
    @mock.patch("worker.Worker._recvall")
    @mock.patch("struct.unpack")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_master_reduce_location(self, mock_sel, mock_sock, mock_unpack, mock_recvall):
        self.worker_node = worker.Worker(testing=True)
        mock_key = mock.Mock(name="key")
        mock_unpack.side_effect = [(constants.REDUCE_LOCATION_INFO,), (20,), (8,), (55555,)]
        mock_recvall.return_value.decode.return_value = "hostasdf"
        status = self.worker_node.service_master_connection(mock_key, selectors.EVENT_READ)
        self.assertEqual(self.worker_node.request_intermediate_from.get(), (20, "hostasdf",55555))
        self.assertEqual(status, False)

    @mock.patch("builtins.print")
    @mock.patch("worker.Worker._recvall")
    @mock.patch("struct.unpack")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_master_invalid_op(self, mock_sel, mock_sock, mock_unpack, mock_recvall, mock_print):
        self.worker_node = worker.Worker(testing=True)
        mock_key = mock.Mock(name="key")
        mock_unpack.return_value = "INVALID_OPCODE"
        status = self.worker_node.service_master_connection(mock_key, selectors.EVENT_READ)
        mock_print.assert_called_with("ERROR: Invalid opcode received from master node")

    @mock.patch("worker.Worker._recvall")
    @mock.patch("struct.unpack")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_master_write(self, mock_sel, mock_sock, mock_unpack, mock_recvall):
        self.worker_node = worker.Worker(testing=True)
        self.assertEqual(self.worker_node.write_to_master_queue.get(), b'\x00\x00\x00\x00\x00\x00\x00\x01')
        self.worker_node.write_to_master_queue.put(b'\x00\x00\x00\x00\x00\x00\x00\x01')
        self.worker_node.write_to_master_queue.put(b'\x00\x00\x00\x00\x00\x00\x00\x02')
        self.worker_node.write_to_master_queue.put(b'\x00\x00\x00\x00\x00\x00\x00\x03')
        mock_key = mock.Mock(name="key")
        status = self.worker_node.service_master_connection(mock_key, selectors.EVENT_WRITE)
        self.assertEqual(self.worker_node.write_to_master_queue.empty(), True)
        self.worker_node.master_sock.sendall.assert_has_calls([call(b'\x00\x00\x00\x00\x00\x00\x00\x01'), call(b'\x00\x00\x00\x00\x00\x00\x00\x02'), call(b'\x00\x00\x00\x00\x00\x00\x00\x03')])
        self.assertEqual(self.worker_node.bytes_sent, len(b'\x00\x00\x00\x00\x00\x00\x00\x01')*3)
        self.assertEqual(status, False)
        
    @mock.patch("builtins.open")
    @mock.patch("worker.Worker._recvall")
    @mock.patch("struct.unpack")
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_worker_read(self, mock_sel, mock_sock, mock_unpack, mock_recvall, mock_open):
        self.worker_node = worker.Worker(testing=True)
        mock_key_1 = mock.Mock(name="key1")
        mock_sock_1 = mock.Mock(name="sock1")
        mock_key_1.fileobj = mock_sock_1
        mock_key_1.data.write_to_worker_queue = queue.Queue()

        #worker requests map results
        mock_unpack.side_effect = [(constants.MAP_RESULTS_REQUEST,), (1,), (1,)]
        mock_open.return_value.read.return_value = struct.pack('>Q', constants.MAP_RESULTS_REQUEST)
        self.worker_node.service_worker_read(mock_key_1, selectors.EVENT_READ)
        mock_open.assert_called_with("mr-1-1.json",'rb')
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 1)

        #worker sends map results
        mock_unpack.side_effect = [(constants.MAP_RESULTS,), (1,), (1000,)]
        mock_recvall.return_value = "FILLER MAP RESULTS"
        self.worker_node.service_worker_read(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(self.worker_node.intermediate_results.get(), (1,"FILLER MAP RESULTS"))

        #worker disconnects
        mock_recvall.return_value = None
        self.worker_node.service_worker_read(mock_key_1, selectors.EVENT_READ)
        mock_sel.return_value.unregister.assert_called_with(mock_sock_1)
        mock_sock_1.close.assert_called_once()
        
    @mock.patch("socket.socket")
    @mock.patch("selectors.DefaultSelector")
    def test_service_worker_write(self, mock_sel, mock_sock):
        self.worker_node = worker.Worker(testing=True)
        mock_key_1 = mock.Mock(name="key1")
        mock_sock_1 = mock.Mock(name="sock1")
        mock_key_1.fileobj = mock_sock_1
        mock_key_1.data.write_to_worker_queue = queue.Queue()
        test_msg = struct.pack('>Q', constants.MAP_RESULTS_REQUEST)
        mock_key_1.data.write_to_worker_queue.put(test_msg)
        
        self.worker_node.service_worker_write(mock_key_1, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_1.data.write_to_worker_queue.empty(), True)
        mock_sock_1.sendall.assert_called_with(test_msg)
        self.assertEqual(self.worker_node.bytes_sent, len(test_msg))
        
class TestMRJob(unittest.TestCase):
    @mock.patch("time.sleep")
    @mock.patch("selectors.DefaultSelector")
    @mock.patch("socket.socket")
    def test_run(self, mock_socket, mock_selector, mock_sleep): 
        self.master_node = master.MRJob(4, 2)
        self.master_node.run(True)
        mock_socket.return_value.bind.assert_called_once_with((constants.MASTER_HOST, constants.MASTER_PORT))

        mock_socket.return_value.listen.assert_called_once()
        mock_socket.return_value.setblocking.assert_called_once_with(False)
        mock_selector.return_value.register.assert_called_once_with(mock_socket.return_value, selectors.EVENT_READ, data=None)

    @mock.patch("builtins.print")
    @mock.patch("selectors.DefaultSelector")
    @mock.patch("socket.socket")
    def test_accept_worker_connection(self, mock_socket, mock_selector, mock_print): 
        self.master_node = master.MRJob(4, 2)
        self.master_node.run(True)
        mock_conn = mock.Mock(name="conn")
        mock_socket.return_value.accept.return_value = (mock_conn, ("hostasdf", 1234))
        self.master_node.accept_worker_connection()
        mock_socket.return_value.setblocking.assert_called_once_with(False)
        mock_selector.return_value.register.assert_called_with(mock_conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data=types.SimpleNamespace(write_to_worker_queue=ANY, worker_addr=("hostasdf", 1234)))
        self.assertEqual(self.master_node.worker_connections, {("hostasdf", 1234): mock_conn})

    """
    For testing, we chose to write the service_worker_connection test as one large test because the setup between steps would be excessive otherwise. 
    This is because in addition to testing the individual operations, we also want to test that the state is maintained correctly between steps,
    so it makes sense to test the service_worker_connection function holistically as if it were operating on an actual MapReduce job
    """
    @mock.patch("selectors.DefaultSelector")
    @mock.patch("struct.unpack")
    @mock.patch("master.MRJob._recvall")
    def test_service_worker_connection_all(self, mock_recvall, mock_unpack, mock_selector): 
        self.master_node = master.MRJob(2, 2)
        self.master_node.run(True)
        mock_key_1 = mock.Mock(name="key1")
        mock_key_2 = mock.Mock(name="key2")
        mock_key_3 = mock.Mock(name="key3")
        mock_key_4 = mock.Mock(name="key4")
        mock_sock_1 = mock.Mock(name="sock1")
        mock_sock_2 = mock.Mock(name="sock2")
        mock_sock_3 = mock.Mock(name="sock3")
        mock_sock_4 = mock.Mock(name="sock4")
        mock_key_1.fileobj = mock_sock_1
        mock_key_2.fileobj = mock_sock_2
        mock_key_3.fileobj = mock_sock_3
        mock_key_4.fileobj = mock_sock_4
        self.master_node.worker_connections[('host1',1234)]=mock_sock_1
        self.master_node.worker_connections[('host2',4321)]=mock_sock_2
        self.master_node.worker_connections[('host3',2143)]=mock_sock_3
        mock_key_1.data.write_to_worker_queue = queue.Queue()
        mock_key_2.data.write_to_worker_queue = queue.Queue()
        mock_key_3.data.write_to_worker_queue = queue.Queue()
        mock_key_4.data.write_to_worker_queue = queue.Queue()
        mock_key_1.data.worker_addr = ('host1',1234)
        mock_key_2.data.worker_addr = ('host2',4321)
        mock_key_3.data.worker_addr = ('host3',2143)
        mock_key_4.data.worker_addr = ('host4',3412)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 2)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 2)

        #workers 1, 2 get map tasks 1 and 2, worker 3 gets reduce task 1
        mock_unpack.return_value = (constants.REQUEST_TASK,)
        status = self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 1)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1})
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 1)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.assertEqual(self.master_node.completed_tasks, 0)

        status = self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1, mock_key_2.data.worker_addr: 2})
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 1)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.assertEqual(self.master_node.completed_tasks, 0)
        
        status = self.master_node.service_worker_connection(mock_key_3, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 1)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1, mock_key_2.data.worker_addr: 2})
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {mock_key_3.data.worker_addr:1})
        self.assertEqual(mock_key_3.data.write_to_worker_queue.qsize(),1)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.master_node.service_worker_connection(mock_key_3, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.assertEqual(self.master_node.completed_tasks, 0)

        #worker 1 finishes map task 1, requests reduce task 2
        mock_unpack.side_effect = [(constants.MAP_COMPLETE,), (22222,)]
        status = self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_2.data.worker_addr:2})
        self.assertEqual(self.master_node.completed_map_tasks, {mock_key_1.data.worker_addr:[1]})
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks_locations, {1:('host1', 22222)})
        self.assertEqual(self.master_node.completed_tasks, 1)
        mock_sock_3.sendall.assert_called_with(b'\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x05host1\x00\x00\x00\x00\x00\x00V\xce')

        mock_unpack.side_effect = None
        mock_unpack.return_value = (constants.REQUEST_TASK,)
        status = self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_2.data.worker_addr: 2})
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {mock_key_3.data.worker_addr: 1, mock_key_1.data.worker_addr:2})
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(),2)
        self.assertEqual(self.master_node.completed_map_tasks, {mock_key_1.data.worker_addr:[1]})
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_WRITE)
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 0)
        
        #all tasks assigned, worker 1 crashes, meaning map task 1 and reduce task 2 need to be done
        mock_recvall.return_value = None
        status = self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.worker_connections, {mock_key_2.data.worker_addr:mock_sock_2, mock_key_3.data.worker_addr:mock_sock_3})
        self.assertEqual(self.master_node.available_map_tasks.qsize(),1)
        self.assertEqual(self.master_node.completed_map_tasks_locations, {}) 
        self.assertEqual(self.master_node.completed_tasks, 0)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(),1)
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {mock_key_3.data.worker_addr:1})
        mock_selector.return_value.unregister.assert_called_with(mock_sock_1)
        mock_sock_1.close.assert_called_once()

        #worker 2 finishes map task 2, requests map task 1, finishes map task 1, and requests reduce task 2
        mock_unpack.side_effect = [(constants.MAP_COMPLETE,), (33333,)]
        mock_recvall.return_value = 0  #filler value
        status = self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.in_progress_map_tasks, {})
        self.assertEqual(self.master_node.completed_map_tasks, {mock_key_2.data.worker_addr:[2]})
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks_locations, {2:('host2', 33333)})
        self.assertEqual(self.master_node.completed_tasks, 1)
        mock_sock_3.sendall.assert_called_with(b'\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x05host2\x00\x00\x00\x00\x00\x00\x825')

        mock_unpack.side_effect = None
        mock_unpack.return_value = (constants.REQUEST_TASK,)
        status = self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 1)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_2.data.worker_addr:1})
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {mock_key_3.data.worker_addr: 1})
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(),1)
        self.assertEqual(self.master_node.completed_map_tasks, {mock_key_2.data.worker_addr:[2]})
        self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 0)

        mock_unpack.side_effect = [(constants.MAP_COMPLETE,), (33333,)]
        status = self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.in_progress_map_tasks, {})
        self.assertEqual(self.master_node.completed_map_tasks, {mock_key_2.data.worker_addr:[2,1]})
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks_locations, {1:('host2',33333), 2:('host2', 33333)})
        self.assertEqual(self.master_node.completed_tasks, 2)
        mock_sock_3.sendall.assert_called_with(b'\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x05host2\x00\x00\x00\x00\x00\x00\x825')

        mock_unpack.side_effect = None
        mock_unpack.return_value = (constants.REQUEST_TASK,)
        status = self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {})
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {mock_key_2.data.worker_addr:2, mock_key_3.data.worker_addr: 1})
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(),3)
        self.assertEqual(self.master_node.completed_map_tasks, {mock_key_2.data.worker_addr:[2,1]})
        self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_WRITE)
        self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_WRITE)
        self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 0)
        
        #worker 4 starts up late and requests a task, but there are none left
        self.master_node.worker_connections[('host4',3412)]=mock_sock_4
        mock_unpack.return_value = (constants.REQUEST_TASK,)
        status = self.master_node.service_worker_connection(mock_key_4, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {})
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {mock_key_2.data.worker_addr:2, mock_key_3.data.worker_addr: 1})
        self.assertEqual(mock_key_4.data.write_to_worker_queue.qsize(),1)
        self.assertEqual(self.master_node.completed_map_tasks, {mock_key_2.data.worker_addr:[2,1]})
        self.master_node.service_worker_connection(mock_key_4, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_4.data.write_to_worker_queue.qsize(), 0)
        
        # workers 2 and 3 finish their reduce tasks
        mock_unpack.return_value = (constants.REDUCE_COMPLETE,)
        status = self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_READ)
        self.assertEqual(status, False)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {})
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {mock_key_3.data.worker_addr: 1})
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(),0)
        self.assertEqual(self.master_node.completed_map_tasks, {mock_key_2.data.worker_addr:[2,1]})
        self.assertEqual(self.master_node.completed_tasks, 3)

        mock_unpack.return_value = (constants.REDUCE_COMPLETE,)
        status = self.master_node.service_worker_connection(mock_key_3, selectors.EVENT_READ)
        self.assertEqual(status, True)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {})
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {})
        self.assertEqual(self.master_node.completed_tasks, 4)
        mock_sock_2.sendall.assert_called_with(b'\x00\x00\x00\x00\x00\x00\x00\t')
        mock_sock_3.sendall.assert_called_with(b'\x00\x00\x00\x00\x00\x00\x00\t')
        mock_sock_4.sendall.assert_called_with(b'\x00\x00\x00\x00\x00\x00\x00\t')
        mock_selector.return_value.unregister.assert_has_calls([call(mock_sock_2), call(mock_sock_3), call(mock_sock_4)])     
        mock_sock_2.close.assert_called_once()
        mock_sock_3.close.assert_called_once()
        mock_sock_4.close.assert_called_once()


    def test_recvall(self):
        self.master_node = master.MRJob(2, 2)
        mock_sock = mock.Mock(name="sock")
        mock_sock.recv.side_effect = [b"abcd", b"efgh", b"ij"]
        ans = self.master_node._recvall(mock_sock, 10)
        self.assertEqual(ans, b"abcdefghij")

if __name__ == '__main__':
    unittest.main()

from unittest import mock 
from unittest.mock import patch, call, ANY
import socket 
import selectors 
import user 
import master
import constants 
import struct 
import queue 
import types 
import unittest 
import worker


class TestWorker(unittest.TestCase): 
    """
    @mock.patch("builtins.print")
    @mock.patch("selectors.DefaultSelector")
    @mock.patch("socket.socket")
    def test_accept_worker_connection(self, mock_socket, mock_selector, mock_print): 
        worker_1 = worker.Worker(testing=True)
        mock_conn = mock.Mock(name="conn")
        mock_socket.return_value.accept.return_value = (mock_conn, ("hostasdf", 1234))
        worker_1.accept_worker_connection()
        mock_conn.setblocking.assert_called_once_with(False)

        read_sel = mock_selector.return_value 
        write_sel = mock_selector.return_value 
        write_sel.register.assert_called_with(mock_conn, selectors.EVENT_WRITE, data=types.SimpleNamespace(addr=("hostasdf", 1234), write_to_worker_queue=ANY))
        # read_sel.register.assert_called_with(mock_conn, selectors.EVENT_READ, data=types.SimpleNamespace(addr=("hostasdf", 1234), write_to_worker_queue=ANY))
    """

    @mock.patch("socket.socket")
    @mock.patch("struct.unpack")
    @mock.patch("worker.Worker")
    @mock.patch("selectors.DefaultSelector")
    def test_service_master_connection(self, mock_selector, mock_worker, mock_unpack, mock_socket): 
        mock_key = mock.Mock(name="key")
        mock_sock = mock.Mock(name="sock")
        mock_key.fileobj = mock_sock

        mock_master = mock_socket.return_value
        mock_worker.master_sock = mock_master
        mock_unpack.return_value = (constants.ALL_TASKS_COMPLETE,)

        mock_worker.service_master_connection(mock_key, selectors.EVENT_READ)
        mock_selector.return_value.unregister.assert_called_with(mock_worker.master_sock)
        # mock_worker.master_sock.return_value.close.assert_called_once()
        


class TestMRJob(unittest.TestCase): 
    @mock.patch("builtins.print")
    @mock.patch("time.sleep")
    @mock.patch("selectors.DefaultSelector")
    @mock.patch("socket.socket")
    def test_run(self, mock_socket, mock_selector, mock_sleep, mock_print): 
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

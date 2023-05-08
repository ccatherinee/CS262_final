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

    @mock.patch("struct.unpack")
    @mock.patch("master.MRJob._recvall")
    def test_service_worker_connection(self, mock_recvall, mock_unpack): 
        self.master_node = master.MRJob(2, 2)
        self.master_node.run(True)
        mock_key_1 = mock.Mock(name="key1")
        mock_key_2 = mock.Mock(name="key2")
        mock_key_3 = mock.Mock(name="key3")
        mock_sock_1 = mock.Mock(name="sock1")
        mock_sock_2 = mock.Mock(name="sock2")
        mock_sock_3 = mock.Mock(name="sock3")
        mock_key_1.fileobj = mock_sock_1
        mock_key_2.fileobj = mock_sock_2
        mock_key_3.fileobj = mock_sock_3
        self.master_node.worker_connections[('host1',1234)]=mock_sock_1
        self.master_node.worker_connections[('host2',4321)]=mock_sock_2
        self.master_node.worker_connections[('host3',2143)]=mock_sock_3
        mock_key_1.data.write_to_worker_queue = queue.Queue()
        mock_key_2.data.write_to_worker_queue = queue.Queue()
        mock_key_3.data.write_to_worker_queue = queue.Queue()
        mock_key_1.data.worker_addr = ('host1',1234)
        mock_key_2.data.worker_addr = ('host2',4321)
        mock_key_3.data.worker_addr = ('host3',2143)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 2)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 2)

        #map tasks available, first two workers get map tasks, last worker gets reduce task
        mock_unpack.return_value = (constants.REQUEST_TASK,)
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 1)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1})
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 1)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.assertEqual(self.master_node.completed_tasks, 0)

        self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_READ)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1, mock_key_2.data.worker_addr: 2})
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 1)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.master_node.service_worker_connection(mock_key_2, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.assertEqual(self.master_node.completed_tasks, 0)
        
        self.master_node.service_worker_connection(mock_key_3, selectors.EVENT_READ)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 1)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1, mock_key_2.data.worker_addr: 2})
        self.assertEqual(mock_key_3.data.write_to_worker_queue.qsize(),1)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.master_node.service_worker_connection(mock_key_3, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.assertEqual(self.master_node.completed_tasks, 0)

        #one worker finishes map task and requests the final reduce task
        mock_unpack.side_effect = [(constants.MAP_COMPLETE,), (22222,)]
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_2.data.worker_addr:2})
        self.assertEqual(self.master_node.completed_map_tasks, {mock_key_1.data.worker_addr:[1]})
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks_locations[1], ('host1', 22222))
        self.assertEqual(self.master_node.completed_tasks, 1)
        mock_sock_3.sendall.assert_called_with(b'\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x05host1\x00\x00\x00\x00\x00\x00V\xce')
 
        mock_unpack.return_value = (constants.REQUEST_TASK,)
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 1)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1, mock_key_2.data.worker_addr: 2})
        self.assertEqual(mock_key_3.data.write_to_worker_queue.qsize(),1)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.master_node.service_worker_connection(mock_key_3, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_2.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks, {})
        self.assertEqual(self.master_node.completed_tasks, 0)
        
        #reduce task available
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1})
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {mock_key_1.data.worker_addr: 1})
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 1)
        self.assertEqual(self.master_node.completed_tasks, 0)
        
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_WRITE)
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 0)
        self.assertEqual(self.master_node.completed_map_tasks, {})

        #no tasks available
        self.master_node.service_worker_connection(mock_key_1, selectors.EVENT_READ)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1})
        self.assertEqual(self.master_node.in_progress_reduce_tasks, {mock_key_1.data.worker_addr: 1})
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key_1.data.worker_addr: 1})
        self.assertEqual(mock_key_1.data.write_to_worker_queue.qsize(), 1)

        

        mock_unpack.return_value = (constants.REDUCE_COMPLETE)
        
if __name__ == '__main__':
    unittest.main()

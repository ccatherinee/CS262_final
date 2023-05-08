import unittest
from unittest import mock 
from unittest.mock import patch, call, ANY
import socket 
import selectors 
from worker import * 
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
        self.master_node = master.MRJob(1, 1)
        self.master_node.run(True)
        mock_unpack.return_value = (constants.REQUEST_TASK,)
        mock_key, mock_sock = mock.Mock(name="key"), mock.Mock(name="sock")
        mock_key.fileobj = mock_sock 
        mock_key.data.write_to_worker_queue = queue.Queue()
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 1)
        self.master_node.service_worker_connection(mock_key, selectors.EVENT_READ)
        self.assertEqual(self.master_node.available_map_tasks.qsize(), 0)
        self.assertEqual(self.master_node.in_progress_map_tasks, {mock_key.data.worker_addr: 1})
        self.assertEqual(mock_key.data.write_to_worker_queue.qsize(), 1)
        
        self.assertEqual(self.master_node.available_reduce_tasks.qsize(), 1)
        self.master_node.service_worker_connection(mock_key, selectors.EVENT_READ)
        
        

if __name__ == '__main__':
    unittest.main()
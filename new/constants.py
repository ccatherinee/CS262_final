# Opcodes for the wire protocol

# Sent from workers to master
REQUEST_TASK = 1
MAP_COMPLETE = 2
REDUCE_COMPLETE = 3

# Sent from master to workers
NO_AVAILABLE_TASK = 5
MAP_TASK = 6
REDUCE_TASK = 7
REDUCE_LOCATION_INFO = 8
ALL_TASKS_COMPLETE = 9

# Sent from workers to workers
MAP_RESULTS_REQUEST = 11
MAP_RESULTS = 12


# General constants
import socket
MASTER_HOST = socket.gethostbyname(socket.gethostname()) # get user machine's IP address
MASTER_PORT = 12345

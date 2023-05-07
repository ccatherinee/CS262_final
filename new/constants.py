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
MASTER_HOST = "192.168.1.4" # THIS MUST BE SET TO USER'S IP ADDRESS
MASTER_PORT = 12345
INITIAL_DELAY = False # whether to delay assigning tasks in master node for 10 seconds to allow all workers to connect first, for testing purposes only

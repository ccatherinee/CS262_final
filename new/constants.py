# Any imports used in the map and reduce function MUST be imported here
from string import *


# Opcodes for the wire protocol

# Sent from workers to master
REQUEST_TASK = 1 # worker requests a task
MAP_COMPLETE = 2 # worker notifies master that it has completed a map task, sending its listening port
REDUCE_COMPLETE = 3 # worker notifies master that it has completed a reduce task

# Sent from master to workers
NO_AVAILABLE_TASK = 5 # master notifies worker that there are no available tasks
MAP_TASK = 6 # master assigns a map task to a worker, sending the map task number, M, R, the mapper function, and the map text file input (the latter two encoded as bytes)
REDUCE_TASK = 7 # master assigns a reduce task to a worker, sending the reduce task number, M, R, and the reducer function (encoded as bytes)
REDUCE_LOCATION_INFO = 8 # master sends the location of map task result to a reduce worker, sending the completed map task number and the IP address and listening port of the worker that has the map results
ALL_TASKS_COMPLETE = 9 # master notifies worker that all tasks are complete

# Sent from workers to workers
MAP_RESULTS_REQUEST = 11 # worker requests the results of a map task from another worker, sending which (map task, partition) results it wants
MAP_RESULTS = 12 # worker sends the results of a map task to another worker, sending which map task number it corresponds to


# General constants
MASTER_HOST = "10.250.124.104" # this must be set to the user's computer's IP address
MASTER_PORT = 12355 # the port on which the master node listens for connections
INITIAL_DELAY = False # whether to delay assigning tasks in master node for 10 seconds to allow all workers to connect first, for testing purposes only

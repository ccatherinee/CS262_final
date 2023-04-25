WORKER_PORT_START = 12435

# LB -> WORKER
MAP = 0 
SHUFFLE = 4
REDUCE = 12

# worker -> LB
MAP_CONFIRM = MAP + 1
SHUFFLE_CONFIRM = SHUFFLE + 1 
REDUCE_CONFIRM = REDUCE + 1
SHUFFLE_ERROR = SHUFFLE + 2

# LB -> worker
SHUFFLE_UPDATE = SHUFFLE + 3
SHUFFLE_STATE_REQUEST = SHUFFLE + 4

# if you encounter a situation in which you're unable to send something to a fellow worker socket, then send the lb a shuffle_error with the socket you tried to sent to and failed 
# socket gets SHUFFLE_ERROR, failed_socket_port and replaces the failed_socket_port with a new_socket_port and updates all the worker nodes SHUFFLE_UPDATE failed_port new_port 
# upon received SHUFFLE_UPDATE, worker nodes should just replce the failed_port with new_port and resend 

# no need for SHUFFLE_ERROR actually, if worker encounters an error whil
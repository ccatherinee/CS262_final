WORKER_PORT_START = 11116

MAP = 0 
REDUCE = 4
HEARTBEAT = 8

MAP_CONFIRM = MAP + 1 
REDUCE_CONFIRM = REDUCE + 1 
REQUEST = REDUCE + 2 
GIVE = REDUCE + 3
HEARTBEAT_CONFIRM = HEARTBEAT + 1
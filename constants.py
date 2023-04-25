WORKER_PORT_START = 12260

# LB -> WORKER
MAP = 0 
SHUFFLE = 4
REDUCE = 12

# worker -> LB
MAP_CONFIRM = MAP + 1
SHUFFLE_CONFIRM = SHUFFLE + 1 
REDUCE_CONFIRM = REDUCE + 1
SHUFFLE_FAILURE = SHUFFLE + 2

# LB -> worker
SHUFFLE_UPDATE = SHUFFLE + 3
SHUFFLE_STATE_REQUEST = SHUFFLE + 4


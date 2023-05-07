# Introduction
This is a MapReduce Python system/library. The newest version of our code can be found in the new folder.

# Requirements
Install the latest version of Python and the dill module (an extension of Python's pickle module) via pip install dill.

Before running anything using Python, set the environment variable PYTHONHASHSEED via the command export PYTHONHASHSEED=0 in the terminal for macOS or set PYTHONHASHSEED=0 for windows, on any terminals used in both the user machine and on any of the worker machines (e.g., if using two separate terminals on one machine, you must set this environment variable to 0 twice). This is to ensure the hash function used is consistent across machines, disabling the randomization that Python uses for its built-in hash function.

(Throughout this README, we will refer to the user program as being in the file user.py. The user program does not strictly have to be named user.py - the following directions apply to any other user program file name too.)

Ensure mr-input-1.txt through mr-input-M.txt are the map task input splits, where M is the number of map tasks as defined in the user program, is in the same folder as the user program user.py.

In the user program user.py, the map and reduce functions must return iterables / generators (e.g., a list, or generated values via yield like in the given example code). Additionally, any imported libraries / functions that the user-defined map and reduce function use must be imported in constants.py, which should also be in the same directory as master.py, worker.py, and user.py.

You must set MASTER_HOST to your machine's IP address before running the MapReduce job, and set MASTER_PORT to whatever port you want your master node to run on.

# How to Use
The newest version of our MapReduce system/library lies in the new folder. From there, run python user.py to start up the user program invoking MapReduce and to start up the master node. After that, for as many worker machines as wanted to be started, run python worker.py on any machine to start up a worker node to be used in the MapReduce computation. You must start user.py first before starting workers.

As mentioned above, the input files must be in the same directory as user.py, and must be in the format mr-input-X.txt (utf-8 encoded) for X ranging from 1 to M, where M is the number of map splits defined in user.py. The output files will be text files found on each of the reduce worker machines, labeled mr-output-Y.txt for Y ranging from 1 to R, where R is the number of reduce partitions defined in user.py.

# Testing 
Unit tests are included in tests.py. For manual integration and end-to-end testing, specifically for fault tolerance, it is useful to programatically crash worker nodes. This can be done when each worker node is initially started: python worker.py die_map makes that specific worker die during a map task, while python worker.py die_reduce makes that worker die during a reduce task. The master and worker node logs can then be examined to see if the appropriate fault tolerance features are activated (e.g., reassigning tasks of the dead worker node).

# Final Write-up and Engineering Notebook
For more clarity on design and more discussion on our MapReduce system, see the final write-up
(containing our engineering notebook), located at https://docs.google.com/document/d/1aoQZ7dhpFFf5QATdoE5dXEF7W8QXflFaidOS2B3m_0U/edit?usp=sharing.
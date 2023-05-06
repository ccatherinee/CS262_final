# Requirements
Install the latest version of Python and the dill module (an extension of Python's pickle module) via pip install dill.

# How to Run
The newest version of our design/code lies in the new folder. From there, use:
python user.py to start up the user program invoking MapReduce and to start up the master node.
python worker.py on any machine to start up at least one worker node to be used in the MapReduce computation.

Ensure mr-input-1.txt through mr-input-M.txt, where M is the number of map tasks, is in the same folder as the user program user.py.

User's map and reduce functions must return iterables / generators (e.g., a list, or generated values via yield like in the given example code).

Users must start user.py first before starting workers.

# Final Write-up and Engineering Notebook

The final write-up (containing our engineering notebook) is located at https://docs.google.com/document/d/1aoQZ7dhpFFf5QATdoE5dXEF7W8QXflFaidOS2B3m_0U/edit?usp=sharing.
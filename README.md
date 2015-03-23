##Distributed Sorting Framework
A simple fault-tolerant / load-balanced distributed sorting framework

###Design
There are three main components of this Sorting framework:
Client, Server, and ComputeNode

1. Client (src/Client.java)
  * The users interact with the Client UI, there are two main options: Submit Jobs and Display Statistics. Submit Jobs include entering the filename, the Client will read this file and parse its input into a String, then send it along with the Sort request to the Server, waiting for the result.

  * The Display Statistics send Stat request to the Server, who will aggregate the system and node statistics and send back for the user to view.

2. Server (src/Server.java)
  * The server waits on its port for connection/messages from both the Client and the Compute Nodes, it also actively communicate with the Compute Nodes during the Sorting process. Every time a connection comes, it spawns a new thread to handle the connection. The thread will parse the incoming message and handle depending on its content.   
  
  * The server accepts the Sort Request and the Stat request from the Client. After accepting the Sort request, it divides the Job into (Total Number of Compute Nodes - 2) portions. It leaves 2 Free Compute Nodes for the purpose of Load Balancing later. Server sends each portion/task to a Work Compute Node to sort, it keeps track of these assigned nodes. Then it starts the Job Tracking process.

  * This Job Tracking sends heartbeat messages to assigned Nodes periodically to check their status, if the status is Running or Finished then that node is fine. If the status is Moved/anotherNode then the Assigned Node table is update with this new node information (the task has migrated). If the status is Down then the Compute Node has failed and the Server will Search for another available Node to re-execute this Task. The Search for another node looks for any node that is NOT Running, Reserved, Moved, nor Down. This search keeps going until it finds one (even if none is available, a busy node will eventually finish, a Down node will eventually restart). The server will re-send the task to this found Node and update its Assigned Node table to track it later.

  * The Server acknowledges that a task has been completed when it receives an ImFinished message from the Compute Node. The first one to finish is assigned as the Merger Node, each subsequent Compute Node sending in their Finish message will be notified of the Merger Node address.

  * The Server wait until it receives the Final Merged result from the Merger Node. It will tell all compute nodes to cleanup, it computes the running time then send back the result to the waiting Client.
	
  * For the Stat request, the Server queries all Compute Nodes to get their invididual statistics (number of faults, received tasks, migrated tasks, current and average loads), then aggregate them, and send these back along with other system statistics (total jobs, total tasks, redundant tasks) it keeps, to the Client. 

3. Compute Node (src/ComputeNode.java)
  * The ComputeNode receives Sorting request and sorts the list. It waits on its port and spawns a new thread to handle each new connection. The thread also parses incoming messages and handle them depending on the content.
	
  * Upon startup, the Compute Node contacts the Server to be added to the Node List. It reads the Config file to set the Load Threshold and Fail probability parameter. It maintains its Status for management purpose. The status can be: (1) Ready: Node is free and can accept new task (2) Running: Node is currently sorting numbers and not available (3) Down: node is down and will restart in 3 seconds (4) Reserved: Server or another node has reserved this node to send its task soon (5) Finished: node has finished sorting and is available for new task (6) Moved: Node has migrated its task to another node.
	
  * When it receives the Sort request, it will first check if its current load is above the threshold, if yes it will check other nodes which have loads below the threshold to migrate task to. If none is found, it will run the task locally. Now it randomizes to check for fail probability, if it is supposed to fail, its status changes to Down, and it stops sorting. After 3 seconds, the node restarts by setting its status to Ready.
	
  * Once it finishes Sorting, it will notify the Server, receiving back a Merger Node address, it will send the sorted list to this Merger Node. The Merger Node will receive and combine sorted list, the try to merge them. The Merging phase also checks for current load against Threshold, if it fails, this Merging Tasks also needs to be migrated to another node below the threshold (by sending the Full Combined list). The final Merger Node that meets the requirement will do the actual Merging, and upon finish, send this back to the Server.
	
  * Each compute node keeps statistics of number of tasks it receives, executes, migrates and faults.

###Execution Setup

  Run all three components Client, FileServer and Collector on different machines. The only restriction is that ComputeNode run with different listening ports each, since we use it as their ID. Client and ComputeNodes also must know the Server's machine IP address and its Listening Port. 

  A sample testing environment on multiple machines:

  1. Compilation:

        ./compile.sh
	
  2. Server:
	
        java -cp bin Server [its Listening Port] 

  3. Client:
		
        java -cp bin Client [Server IP] [Server Port] 

  4. ComputeNodes on 10 different machines:

        java -cp bin ComputeNode [Server IP] [Server Port] [its Listening Port]
	

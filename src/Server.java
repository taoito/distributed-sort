import java.io.*;
import java.net.*;
import java.util.*;

public class Server implements Runnable {
	private Socket connection = null;
	static ServerSocket serverSocket = null;
	// list of all Compute Nodes
	static List<String> computeNodeList = new ArrayList<String>();
	// list of all tasks from the whole Job
	static List<String> taskList = new ArrayList<String>();
	// list of assigned compute node doing each task 
	static List<String> assignedNode = new ArrayList<String>();
	// Number of assigned node that's running or finished (NOT down)
	private static int numOKNode = 0;

	// Number of assigned node that has completed its task
	private static int numCompleted = 0;
	// Assigned Merge Node
	private static String mergeNodeInfo = ""; 
	// All Compute Nodes info, to be sent to each node
	private static String allNodeInfo = "";
	// Number of redundant tasks sent 
	private static int numRedunTasks = 0;
	private static int totalJobs = 0;
	private static int totalTasks = 0;

	private static DataOutputStream toClient;
	//time measurement
	static long start,end;
	double runningTime,runningTimeSort,runningTimeMerge;

	public Server(Socket newConnection){
		this.connection = newConnection;
	}

	public static void main(String args[]) throws Exception{
		if (args.length != 1) {
			System.out.println("Syntax error: Include Server Listening Port");
			System.exit(0);
		}	
		System.out.println("===========================");
		System.out.println("Starting the Server ...");
		System.out.println("===========================");

		int port = Integer.parseInt(args[0]);

		try {
			serverSocket = new ServerSocket( port );
		} catch (IOException e) {
			System.out.println("Could not listen on port " + port);
			System.exit(0);
		}
		while (true) {
			Socket newCon = serverSocket.accept();
			System.out.println("New connection coming ...");
			Runnable runnable = new Server(newCon);
			Thread thread = new Thread(runnable);
			thread.start(); // start new thread to accept each connection
		}
	}

	public void run() {
		try {
		BufferedReader inFromClient =
			new BufferedReader(new InputStreamReader(connection.getInputStream()));
		DataOutputStream out = new DataOutputStream(connection.getOutputStream());
		String received = inFromClient.readLine();
		considerMessage(received,out);

		} catch (Exception e) {
			System.out.println("Thread cannot serve connection");
		}
	}

	// Parse the message sent from either Client or Compute Nodes
	public void considerMessage(String received, DataOutputStream out) throws Exception {
		String[] tokens = received.split("/");
		String response = "";

		// if it's a compute node starting up, add it to the list
		if (tokens[0].equals("imComputeNode")) {
			computeNodeList.add(tokens[1]);
			System.out.println("Current Number of Compute Nodes: " + computeNodeList.size());
			response = "Added you to the Compute Node list";
			out.writeBytes(response + "\n");
			out.close();
		}
		// if it's a client requesting sort, do the job
		else if (tokens[0].equals("sort")) {
			toClient = out;
			totalJobs++;
			sortJobs(tokens[1]); 
		}	
		// if it's a client requesting stat, aggregate the stat and send back
		else if (tokens[0].equals("getStat")) {
			response = aggregateStat(); 
			out.writeBytes(response + "\n");
			out.close();
		}
		// When a compute node has completed, if it's the 1st one, Set it as Merger Node
		// Respond to each with Merger Node info and Number of Tasks in the system
		else if (tokens[0].equals("imFinished")) {
			synchronized (this) {
			System.out.println("Node is finished: " + tokens[1]);
			if (numCompleted == 0) {
				System.out.println("Merge node is assigned to " + tokens[1]);
				mergeNodeInfo = tokens[1];
			}
			numCompleted++;
			response = mergeNodeInfo + "/" + taskList.size();
			out.writeBytes(response + "\n");
			out.close();
			}
		}
		// If it's the Merger Node sending the Merged Result back, Send to Client
		// Reset several variables, cleanUp by telling all compute nodes.
		else if (tokens[0].equals("result")) {
			end = System.nanoTime();
			runningTime = (end - start) / 1000000000.0;
			runningTimeSort = runningTime - Double.parseDouble(tokens[2]);

			System.out.println("Result is here: " + tokens[1]);
			out.writeBytes("Thanks for merging\n");
			out.close();
			toClient.writeBytes(tokens[1] + "/" + runningTime + "/"
				       	+ runningTimeSort + "/" + runningTimeMerge + "\n");
			toClient.close();
			numCompleted = 0;
			numOKNode = 0;
			mergeNodeInfo = "";
			allNodeInfo = ""; 
			taskList.clear();
			assignedNode.clear();
			cleanUp();
			System.out.println("DONE ==============================");
		}
	}

	// Sorting the Job sent from Client, split up into smaller tasks, sent to each Compute Node
	public static void sortJobs(String jobs) throws Exception {
		String[] num = jobs.split(",");

		//List of nodes info
		allNodeInfo = ""; 
		for (int i = 0; i < computeNodeList.size(); i++) 
			allNodeInfo = allNodeInfo + computeNodeList.get(i) + ",";

		//Leave 2 Free Compute Nodes for balancing purpose
		int numWorkNode = computeNodeList.size() - 2;
		
		// Numbers per Node to sort
		int numPerNode = num.length / numWorkNode;
		int numPerNode2 = num.length - (numWorkNode-1) * numPerNode;

		int i = 0;
		String task = "";

		start = System.nanoTime();

		//Send to each Work Compute Nodes a task
		for (i = 0; i < (numWorkNode-1); i++) {
			task = "";
			for (int j = 0; j < numPerNode; j++) 
				task = task + num[i*numPerNode+j] + ",";
			taskList.add(task);
			assignedNode.add(i + "/" + computeNodeList.get(i));
			requestCompute(i,task,allNodeInfo);
		}
			task = "";
			for (int j = 0; j < numPerNode2; j++) 
				task = task + num[i*numPerNode+j] + ",";
			taskList.add(task);
			assignedNode.add(i + "/" + computeNodeList.get(i));
			requestCompute(i,task,allNodeInfo);

		totalTasks = totalTasks + numWorkNode;

		// While not all assigned nodes have been confirmed to be Running/Finished
		// Keep Tracking Tasks at all nodes
		while (numOKNode < assignedNode.size())
			jobTracking();
	}

	//Ask a Compute Node to sort this task, also send along all Nodes Info
	public static void requestCompute(int computeNodeIndex, String task, String allNodeInfo) {
		String[] tokens = computeNodeList.get(computeNodeIndex).split(":"); 
		String nodeIP = tokens[0]; 
		int nodePort = Integer.parseInt(tokens[1]);

		String message = allNodeInfo + "/" + task;

		try{
		Socket sendingSocket = new Socket(nodeIP,nodePort);
		DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		BufferedReader inFromNode = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		out.writeBytes("sort/" + message + "\n");
		System.out.println("Sent jobs to Compute Node ID: " + nodePort); 

		String response = inFromNode.readLine();
		System.out.println(response);

		out.close();
		inFromNode.close();
		sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Node ID " + nodePort);
		}
	}

	// Track all Assigned Nodes by Heartbeat Messages, receive its current Status
	// Skip a node if it has been confirmed to be OK (Running/Finished)
	// If it's DOWN, find any available node and resend the task to re-execute
	// If it's MOVED (due to task migration), edit the Assigned Node location info and track this new node
	// If it's Running or Finished, then it's OK
	public static void jobTracking() throws Exception {
		Thread.sleep(1000);
		for (int i = 0; i < assignedNode.size(); i++) {

			if (assignedNode.get(i).equals("OK"))
				continue;

			String[] tokens = assignedNode.get(i).split("/"); 
			int taskIndex = Integer.parseInt(tokens[0]);
			String[] nodeInfo = tokens[1].split(":");
			String nodeIP = nodeInfo[0]; 
			int nodePort = Integer.parseInt(nodeInfo[1]);

			String check = heartbeatMsg(nodeIP, nodePort);
			String[] status = check.split(";");
			if (status[0].equals("Down")) {
				int newNodeInd = -1; 
				while (newNodeInd == -1) {
					newNodeInd = findAvailableNode();
					Thread.sleep(100);
				}
				requestCompute(newNodeInd,taskList.get(taskIndex),allNodeInfo);
				numRedunTasks++;
				totalTasks++;
				assignedNode.set(i,taskIndex + "/" + computeNodeList.get(newNodeInd));
			}
			else if (status[0].equals("Moved")) {
				assignedNode.set(i,taskIndex + "/" + status[1]);
				i--;
				continue;
			}
			else if (status[0].equals("Running") || status[0].equals("Finished")) {
				assignedNode.set(i,"OK");
				numOKNode++;
			}
		}
	}

	//Find any available compute node (by description, doesn't care about its current load)
	//Node is only available if it's NOT Running, Down, Reserved, Moved 
	public static int findAvailableNode() {
		for (int i = computeNodeList.size()-1; i >= 0; i--) {

			String[] tokens = computeNodeList.get(i).split(":");
			String nodeIP = tokens[0];
			int nodePort = Integer.parseInt(tokens[1]);
			
			String temp = getNodeStatus(nodeIP,nodePort);
			String[] nodeStatus = temp.split(";");
			
			if (nodeStatus[0].equals("Running") || nodeStatus[0].equals("Down")
				       	|| nodeStatus[0].equals("Reserved") || nodeStatus[0].equals("Moved")) 
				continue;

			return i;
		}
		return -1;
	}

	//Clean up after finished, sending all Compute Nodes "done" message
	public static void cleanUp() {
		for (int i = computeNodeList.size()-1; i >= 0; i--) {

			String[] tokens = computeNodeList.get(i).split(":");
			String nodeIP = tokens[0];
			int nodePort = Integer.parseInt(tokens[1]);

			try{
			Socket sendingSocket = new Socket(nodeIP,nodePort);
			DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());

			out.writeBytes("done\n");

			out.close();
			sendingSocket.close(); 
			} catch (Exception e) {
				System.out.println("Cannot connect to the Compute Node ID " + nodePort);
			}
		}
	}

	//Heartbeat message to track Compute Nodes
	public static String heartbeatMsg(String nodeIP, int nodePort) {
		String response = "";

		try{
		Socket sendingSocket = new Socket(nodeIP,nodePort);
		DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		BufferedReader inFromNode = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		out.writeBytes("heartbeat\n");
		System.out.println("Heartbeat, Tracking Compute Node ID: " + nodePort); 

		response = inFromNode.readLine();
		System.out.println("  status: " + response);
		out.close();
		inFromNode.close();
		sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Compute Node ID " + nodePort);
		}
		return response;
	}

	// Find a node status when looking for available nodes
	public static String getNodeStatus(String nodeIP, int nodePort) {
		String response = "";

		try{
		Socket sendingSocket = new Socket(nodeIP,nodePort);
		DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		BufferedReader inFromNode = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		out.writeBytes("find\n");
		System.out.println("Finding availability status of Compute Node ID: " + nodePort); 

		response = inFromNode.readLine();
		System.out.println("  status: " + response);
		out.close();
		inFromNode.close();
		sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Compute Node ID " + nodePort);
		}
		return response;
	}

	// Aggregate all the system statistics, ask all nodes to respond with their stats 
	public static String aggregateStat() {
		int totalFaults = 0;
		int totalMigs = 0;
		String allNodeStat = "";

		for (int i = 0; i < computeNodeList.size(); i++) {
			String[] nodeInfo = computeNodeList.get(i).split(":");
			String nodeIP = nodeInfo[0];
			int nodePort = Integer.parseInt(nodeInfo[1]);
			String response = "";

			try{
			Socket sendingSocket = new Socket(nodeIP,nodePort);
			DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
			BufferedReader inFromNode = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

			out.writeBytes("stat\n");
			response = inFromNode.readLine();

			out.close();
			inFromNode.close();
			sendingSocket.close(); 

			String[] eachStat = response.split(",");
			String thisNodeStat = "Node[" + nodePort + "]:  CurrentLoad = " + eachStat[0]  
					+ "  AvgLoad 5min = " + eachStat[1] + " 15min = " + eachStat[2]
			+ "  NumTasks Received = " + eachStat[3] + " Executed = " + eachStat[4] 
			+ " Migrated = " + eachStat[5] + " Faults = " + eachStat[6]; 

			allNodeStat = allNodeStat + thisNodeStat + ";";
			totalMigs = totalMigs + Integer.parseInt(eachStat[5]);
			totalFaults = totalFaults + Integer.parseInt(eachStat[6]);

			} catch (Exception e) {
				System.out.println("Cannot connect to the Compute Node ID " + nodePort);
			}
		}

		System.out.println("SENDING STAT TO CLIENT");
		String overallStat = allNodeStat + totalJobs + ";" + totalTasks + ";" 
					+ totalFaults + ";" + numRedunTasks + ";" + totalMigs; 
		return overallStat;
	}
}

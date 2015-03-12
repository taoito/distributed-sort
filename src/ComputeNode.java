import java.io.*;
import java.util.*;
import java.net.*;

public class ComputeNode implements Runnable {

	private Socket connection = null;
	static ServerSocket serverSocket = null;

	private	static double failProb = 0.0;
	private	static double threshold = 0.0;	
	private static String allNodeInfo;
	private static String status = "Ready";
	private static String myInfo;
	private static String serverIP;
	private static int serverPort;
	//merged list so far (for Merger Node only)
	private static String mergedList = "";
	private static String finalList = "";
	//total number of tasks in the system
	private static int numTasks;
	//number of sorted partial result received so far (for Merger Node)
	private static int numResultRecv = 0;
	//Merger Node address
	private static String mergeNode = "";

	//Number of received Task for each Compute Node
	private static int numRecvTasks = 0;
	//Number of Task each Compute Node has ran
	private static int numExeTasks = 0;
	//Number of Task each Compute Node has migrated to another node
	private static int numMigs = 0;
	//Number of Faults each Compute Node has;
	private static int numFaults = 0;
	// Busy Lock for synchronization purpose
	static int busy = 0;

	//time measurement
	static long startMerge,endMerge;
	static double runningTimeMerge;

	public ComputeNode(Socket newConnection){
		this.connection = newConnection;
	}

	public static void main(String args[]) throws Exception {
		if (args.length != 3) {
			System.out.println("Syntax error: Include [Server IP] & [Port] & [Compute Node listening Port]");
			System.exit(0);
		}
		System.out.println("=======================================");
		System.out.println("Starting the Compute Node... ID = "+args[2]);
		System.out.println("=======================================");

		serverIP = args[0];
		serverPort = Integer.parseInt(args[1]);
		int myPort = Integer.parseInt(args[2]);
		String myIP = InetAddress.getLocalHost().getHostAddress();
		myInfo = myIP + ":" + myPort;
		contactServer();
		readConfigFile();

		try {
			serverSocket = new ServerSocket(myPort);
		} catch (IOException e) {
			System.out.println("Could not listen on port " + myPort);
			System.exit(0);
		}
		while (true) {
			Socket newCon = serverSocket.accept();
			System.out.println("New connection coming ...");
			Runnable runnable = new ComputeNode(newCon);
			Thread thread = new Thread(runnable);
			thread.start(); // start new thread to accept each connection
		}

	}

	public void run() {
		try {
		    BufferedReader in =
			    new BufferedReader(new InputStreamReader(connection.getInputStream()));
		    DataOutputStream out = new DataOutputStream(connection.getOutputStream());
		    String received = in.readLine();
		    considerMessage(received,out);

		} catch (Exception e) {
			System.out.println("Thread cannot serve connection: " + e);
		}
	}

	//Parse received message and perform appropriate actions
	public void considerMessage(String message, DataOutputStream out) throws Exception {
		String[] tokens = message.split("/");

		// If another Compute Node is checking my status (for Migration)
		if (tokens[0].equals("check")) {

			while (busy == 1) {
				Thread.sleep(100);
			}
				//Lock the status checking
				synchronized (this) {
					busy = 1;
				}
			Thread.sleep(300);
			String curLoad = readCurrentLoad();
			double load = Double.parseDouble(curLoad);
			out.writeBytes(status + ";" + curLoad + "\n");
			out.close();
			System.out.println("Another Node checking my status, returned: " + status);
			//If my status is Ready and my current load is below the threshold
			//That requesting Compute Node will reserve me
			if (status.equals("Ready") && (load <= threshold)) { 
				status = "Reserved";
			}
				//Release the Lock
				synchronized (this) {
					busy = 0;
				}
		}
		// If the server is Finding available nodes and look for my status
		else if (tokens[0].equals("find")) {

			while (busy == 1) {
				Thread.sleep(100);
			}	
				synchronized (this) {
					busy = 1;
				}

			Thread.sleep(300);
			out.writeBytes(status + "\n");
			out.close();
			System.out.println("Server finding my status, returned: " + status);
			//If my status is Ready, the Server will reserve me
			if (status.equals("Ready")) {
				status = "Reserved";
			}
				synchronized (this) {
					busy = 0;
				}
		}
		// If the Server is sending hearbeat message to Track my status
		else if (tokens[0].equals("heartbeat")) {
			out.writeBytes(status + "\n");
			out.close();
			System.out.println("Heartbeat msg from Server, returned: " + status);
			String[] statInfo = status.split(";");
			//If my status is Moved/anotherNode, then now the Server knows it.
			//Change my status to Ready
			if (statInfo[0].equals("Moved")) {
				status = "Ready";
				System.out.println("Server now knows about the migration, node is again " + status);
			}
			//If my status is Down, now the Server knows it, after 3 sec, Node restarts and is Ready
			if (status.equals("Down")) {
				Thread.sleep(3000);
				status = "Ready";
				System.out.println("After 3 sec, Node has restarted and is " + status);
			}
		}
		// If the Server or Another Compute Node is requesting me to Sort a Task
		else if (tokens[0].equals("sort")) {
			//I'm immediately reserved
			status = "Reserved";
			out.writeBytes("Compute Node has received task sent\n");
			out.close();
			numRecvTasks++;
			System.out.println("Got this Unsorted List: " + tokens[2]);
			allNodeInfo = tokens[1];
			sortTask(tokens[2]); 
		}	
		// If another Compute Node is sending me (Merger) their sorted result
		else if (tokens[0].equals("combine")) {
			synchronized (this) {
			System.out.println("MERGER: received partial sorted result: " + tokens[1]);
			mergedList = mergedList + tokens[1] + ";";
			System.out.println("Current Updated List: " + mergedList);
			if (out != null) {
				out.writeBytes("Merger has received your partial result\n");
				out.close();
			}
			numResultRecv++;
			// If I have received all sorted results, will try to Merge them
			if (numResultRecv == numTasks) {
				tryMerging();
			}
			}
		}
		// If the Merger could not merge because of high load, ask me to Merge
		else if (tokens[0].equals("merge")) {
			System.out.println("New Merger: received FULL result: " + tokens[1]);
			mergedList = tokens[1];
			out.writeBytes("New merger has received your full result\n");
			out.close();
			tryMerging();
		}
		// If the Server is asking for my Statistics
		else if (tokens[0].equals("stat")) {
			String nodeAllStat = readLoadStat() + "," + numRecvTasks + ","
					+ numExeTasks + "," + numMigs + "," + numFaults;
			out.writeBytes(nodeAllStat + "\n");
			out.close();
		}
		// If the job is done, the Server asks me to cleanup and be Ready again
		else if (tokens[0].equals("done")) {
			out.close();
			status = "Ready";
			finalList = ""; mergedList = "";
			numResultRecv = 0;
			System.out.println("Done -----------------");
		}
	}

	// Try merging, check if my current load is below the Threshold
	// If not, check for another node with acceptable Threshold to be New Merger then Send
	// If can't find any, I will merge the results.
	public static void tryMerging() {
		String searchResult = "";
		double currentLoad = Double.parseDouble(readCurrentLoad());
		if (currentLoad > threshold) {
			searchResult = checkOtherNodes();
			if (!searchResult.equals("none")) {
				String[] tokens = searchResult.split(":");
				String nodeIP = tokens[0];
				int nodePort = Integer.parseInt(tokens[1]);
				sendFullResult(nodeIP,nodePort,mergedList);
				numMigs++;
				finalList = ""; mergedList = ""; numResultRecv = 0;
				System.out.println("Load balance, migrated Merging task to " + searchResult);
				return;
			}
		}
		startMerging();
	}

	// Start the actual Merging process
	// After finished, send the Final Result to the Server
	public static void startMerging() {
		startMerge = System.nanoTime();
		if (numTasks == 1) {
			finalList = mergedList;
			System.out.println("Final result: " + finalList);
			return;
		}

		System.out.println("Combined list: " + mergedList);

		String[] partialList = mergedList.split(";");
		finalList = partialList[0];
		for (int i = 1; i < partialList.length; i++) {

			String[] numA = finalList.split(","); 
			String[] numB = partialList[i].split(",");
			finalList = "";

			int a = 0, b = 0;
			String next = "";
			while (a < numA.length && b < numB.length) {
				if (Integer.parseInt(numA[a]) <= Integer.parseInt(numB[b])) {
					next = numA[a];
					a++;
				}
				else {
					next = numB[b];
					b++;
				}
				finalList = finalList + next + ","; 
			}	
			if (a < numA.length) {
				for (int j = a; j < numA.length; j++) 
					finalList = finalList + numA[j] + ",";
			}
			else if (b < numB.length) {
				for (int j = b; j < numB.length; j++) 
					finalList = finalList + numB[j] + ",";
			}
		}

		System.out.println("Sending Final list: " + finalList);

		mergedList = ""; numResultRecv = 0;
		endMerge = System.nanoTime();
		runningTimeMerge = (endMerge - startMerge) / 10000000000.0;

		try {
		    Socket sendingSocket = new Socket(serverIP,serverPort);
		    DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		    BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		    out.writeBytes("result/" + finalList + "/" + runningTimeMerge + "\n");

		    String response = inFromServer.readLine();
		    System.out.println(response);
		    out.close();
		    inFromServer.close();
		    sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Server");
			System.exit(0);
		}
	}

	// Perform the Actual sorting for the given list of numbers
	public void sortTask(String input) throws Exception {
		Thread.sleep(1500);
		// If my Current load is above the Threshold, Look for Other Nodes
		// If found one below the Threshold, send/migrate my task over there
		String searchResult = "";
		double currentLoad = Double.parseDouble(readCurrentLoad());
		if (currentLoad > threshold) {
			searchResult = checkOtherNodes();
			if (!searchResult.equals("none")) {
				String[] tokens = searchResult.split(":");
				String nodeIP = tokens[0];
				int nodePort = Integer.parseInt(tokens[1]);
				sendTask(nodeIP,nodePort,input);
				numMigs++;
				System.out.println("Load balance, migrated Sorting Task to " + searchResult);
				status = "Moved;" + searchResult;
				return;
			}
		}

		// If can't find any below the Threshold, perform Sorting here
		Random rand = new Random();
		double prob = rand.nextDouble();
		System.out.println("prob: " + prob);
		// Randomize probability of Failing
		// If Node fails while sorting, my status is Down, return right away
		if (prob < failProb) {
			status = "Down";
			numFaults++;
			return;
		}
		// Else, my status is Running and perform the actual Sorting
		else { 
			status = "Running";

			String[] numStr = input.split(",");
			int[] num = new int[numStr.length];	
			for (int i = 0; i < numStr.length; i++)   
				num[i] = Integer.parseInt(numStr[i]);
			Arrays.sort(num);
			String sortedNum = "";
			for (int i = 0; i < num.length; i++) {
				Thread.sleep(500);
				sortedNum = sortedNum + num[i] + ",";
			}

			System.out.println("My Sorted List: " + sortedNum);
			numExeTasks++;

			// After finish sorting, notify the Server that I'm Finished
			// Receive the Merger Node address in return
			mergeNode = notifyServer();
			System.out.println("Received merger node ID: " + mergeNode);
			// If I'm the Merger, combine lists locally
			if (myInfo.equals(mergeNode)) {
				considerMessage("combine/"+sortedNum,null);
			} // Else, send my sorted result to the Merger to combine
			else {
				String[] merge = mergeNode.split(":");
				sendMyResult(merge[0],Integer.parseInt(merge[1]),sortedNum);
			}
			status = "Finished";
		}
		return;
	}

	// Notify Server that I'm Finished, receive Merger Node address in return
	public static String notifyServer() {
		String mergeNode = "";
		try {
		    Socket sendingSocket = new Socket(serverIP,serverPort);
		    DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		    BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		    out.writeBytes("imFinished/" + myInfo + "\n");

		    String response = inFromServer.readLine();
		    String[] tokens = response.split("/");
		    mergeNode = tokens[0];
		    numTasks = Integer.parseInt(tokens[1]);
		    out.close();
		    inFromServer.close();
		    sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Server");
			System.exit(0);
		}
		return mergeNode;
	}
	
	// Send my sorted result to the Merger Node to combine then merge
	public static void sendMyResult(String nodeIP, int nodePort, String result) {
		try {
		    Socket sendingSocket = new Socket(nodeIP,nodePort);
		    DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		    BufferedReader inFromNode = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		    out.writeBytes("combine/" + result + "\n");

		    String response = inFromNode.readLine();
		    System.out.println(response);
		    out.close();
		    inFromNode.close();
		    sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Node");
			System.exit(0);
		}
	}

	// Migrate, Send Full Result to another Node to merge 
	public static void sendFullResult(String nodeIP, int nodePort, String result) {
		try {
		    Socket sendingSocket = new Socket(nodeIP,nodePort);
		    DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		    BufferedReader inFromNode = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		    out.writeBytes("merge/" + result + "\n");

		    String response = inFromNode.readLine();
		    System.out.println(response);
		    out.close();
		    inFromNode.close();
		    sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Node");
			System.exit(0);
		}
	}

	// Migrate, Send Task to another Compute Node to sort
	public static void sendTask(String nodeIP, int nodePort, String input) {
		try {
		    Socket sendingSocket = new Socket(nodeIP,nodePort);
		    DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		    BufferedReader inFromNode = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		    out.writeBytes("sort/" + allNodeInfo + "/" + input + "\n");

		    String response = inFromNode.readLine();
		    System.out.println(response);
		    out.close();
		    inFromNode.close();
		    sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Node");
			System.exit(0);
		}
	}

	// Check all Other Compute Nodes to find one that is below the Threshold
	// The found node must NOT be Running, Down, Reserved, Moved
	public static String checkOtherNodes() {
		String[] node = allNodeInfo.split(",");
		for (int i = node.length-1; i >= 0; i--) {
			if (node[i].equals(myInfo))
				continue;
			String[] tokens = node[i].split(":");
			String nodeIP = tokens[0];
			int nodePort = Integer.parseInt(tokens[1]);

			String temp = getNodeStatus(nodeIP,nodePort);
			String[] nodeStatus = temp.split(";");
			if (nodeStatus[0].equals("Running") || nodeStatus[0].equals("Down")
				       	|| nodeStatus[0].equals("Reserved") || nodeStatus[0].equals("Moved"))
				continue;

			double load = Double.parseDouble(nodeStatus[1]);
			if (load <= threshold) {
				System.out.println("found Node: " + node[i]);
				return node[i];
			}	
		}
		return "none";
	}

	// Get the Node Status of another Compute Node
	public static String getNodeStatus(String nodeIP, int nodePort) {
		String nodeStatus = "";
		try {
		    Socket sendingSocket = new Socket(nodeIP,nodePort);
		    DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		    BufferedReader inFromNode = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		    out.writeBytes("check\n");

		    nodeStatus = inFromNode.readLine();
		    out.close();
		    inFromNode.close();
		    sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Node");
			System.exit(0);
		}
		return nodeStatus;
	}

	// Contact Server to join the Compute Node lists
	public static void contactServer() {
		try {
		    Socket sendingSocket = new Socket(serverIP,serverPort);
		    DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		    BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		    out.writeBytes("imComputeNode/" + myInfo + "\n");

		    String result = inFromServer.readLine();
		    System.out.println("Server Response: " + result);
		    out.close();
		    inFromServer.close();
		    sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Server");
			System.exit(0);
		}
	}

	// Read the Configuration File to get Threshold and Fail probability
	public static void readConfigFile() {
		try{
			String fileName = "config";
			FileInputStream fstream = new FileInputStream(fileName);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String[] tokens; 
			String strLine;
			while ((strLine = br.readLine()) != null) {
				tokens = strLine.split("=");
				if (tokens[0].equals("FailProb"))
					failProb = Double.parseDouble(tokens[1]);
				else if (tokens[0].equals("Threshold"))
					threshold = Double.parseDouble(tokens[1]);
			}			
			in.close();
			fstream.close();
			br.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	// Read current load of the system
	public static String readCurrentLoad() {
		String currentLoad = "";
		try{
			String fileName = "/proc/loadavg";
			FileInputStream fstream = new FileInputStream(fileName);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine = br.readLine(); 
			String[] tokens = strLine.split(" ");
			currentLoad = tokens[0];

			System.out.println("Current load: " + currentLoad);

			in.close();
			fstream.close();
			br.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return currentLoad ;
	}

	// Read current load and average loads of the system
	public static String readLoadStat() {
		String loadStat = "";
		try{
			String fileName = "/proc/loadavg";
			FileInputStream fstream = new FileInputStream(fileName);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine = br.readLine(); 
			String[] tokens = strLine.split(" ");

			loadStat = tokens[0] + "," + tokens[1] + "," + tokens[2];
			System.out.println("Load stat: " + loadStat);
			System.out.println("------------------");

			in.close();
			fstream.close();
			br.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return loadStat;
	}
}

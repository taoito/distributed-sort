import java.util.*;
import java.io.*;
import java.net.*;

public class Client {


	// Read in contents of input file name
	public static String readFile(String fileName) {
		String contents="";
		try{
			FileInputStream fstream = new FileInputStream(fileName);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				contents = contents + strLine + ",";
			}			
			br.close();
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.out.println(e + "Cannot read from file");
			System.exit(0);
		}
		System.out.println("Read File: ["+contents+"]");
		return contents;
	}

	// Request server to sort this file content
	public static void requestSort(String fileName, String serverIP, int serverPort) {
		String jobs = readFile(fileName);

		try{
		    Socket sendingSocket = new Socket(serverIP,serverPort);
		    DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		    BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		    out.writeBytes("sort/" + jobs + "\n");
		    System.out.println("Running jobs on Compute Nodes ...");
		    String result = inFromServer.readLine();
		    String[] tokens = result.split("/");
		    System.out.println("Done! Results: ");
		    System.out.println(tokens[0]);
		    System.out.println("Total Running Time : " + tokens[1] + " sec");
		    System.out.println("Total Sorting Time : " + tokens[2] + " sec");
		    System.out.println("Total Merging Time : " + tokens[3] + " sec");
		    out.close();
		    inFromServer.close();
		    sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Server");
		}
	}

	// Request server for Compute Node stats and System stats, then print out
	public static void requestStat(String serverIP, int serverPort) {
		String result = "";

		try{
		    Socket sendingSocket = new Socket(serverIP,serverPort);
		    DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
		    BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

		    out.writeBytes("getStat\n");
		    result = inFromServer.readLine();

		    out.close();
		    inFromServer.close();
		    sendingSocket.close(); 
		} catch (Exception e) {
			System.out.println("Cannot connect to the Server");
		}

		String[] statLine = result.split(";");
		System.out.println("-------------------------");
		System.out.println("Compute Node Statistics: ");
		for (int i = 0; i < statLine.length - 5; i++) 
			System.out.println(statLine[i]);
		System.out.println("-------------------------");
		System.out.println("System Statistics: ");

		System.out.println("\tTotal Jobs = " + statLine[statLine.length-5]);
		System.out.println("\tTotal Tasks = " + statLine[statLine.length-4]);
		System.out.println("\tNumber of Faults = " + statLine[statLine.length-3]);
		System.out.println("\tNumber of Redundant Tasks = " + statLine[statLine.length-2]);
		System.out.println("\tNumber of Task Migrations = " + statLine[statLine.length-1]);
	}

	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.out.println("Syntax error: Include Server IP Address and Port");
			System.exit(0);
		}
		DataInputStream din = new DataInputStream(System.in);
		String serverIP = args[0];
		int serverPort = Integer.parseInt(args[1]);

		while (true) {
			System.out.println("");
			System.out.println("Please choose your options: ");
			System.out.println("1 - Enter File Name to Sort");
			System.out.println("2 - Display System Statistics");
			System.out.println("3 - Exit");
			System.out.print("Choice : ");
			String line = din.readLine();

			if (line.equals("1")) {
				System.out.print("Enter filename (from current directory) : ");
				String fileName = din.readLine();
				requestSort(fileName,serverIP,serverPort);
			}
			else if (line.equals("2")) {
				requestStat(serverIP,serverPort);
			}
			else if (line.equals("3")) {
				System.exit(0);
			}
			else {
				System.out.println("Invalid option");
				System.out.println("");
			}
		}
	}
}

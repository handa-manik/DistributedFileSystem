package DistributedFileSystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Manik, Neha
 *
 * DFSReplicate class performs the following functions:
 * 1. Replicates files at the nodes in the ring when the file count does not meet the 
      replication factor
 * 2. Uses sendCommand and WaitResponse to handle the above process
 * Contains helper function for Socket Connection and input/output stream setup
 */

public class DFSReplicate implements Runnable{
	private static String fileName ;
	private static String ipAdr;
	private static ArrayList<Integer> destList = new ArrayList<Integer>();
	private static  Socket srcSocket ;
	private static DataInputStream srcInput;
	private static DataOutputStream srcOutput;
	private static HashMap<String, DFSTableEntry> replicationTable = new HashMap<String, DFSTableEntry>(); 
	
	public DFSReplicate(String fileName, String ipAdr, ArrayList<Integer> destList){
		this.fileName = fileName;
		this.ipAdr = ipAdr;
		this.destList = destList;
		this.replicationTable = replicationTable;
		
		
		System.out.println("The file name " + this.fileName + " will be sent to : " + ipAdr + " for destList : " + destList);
	}
	
	public void run(){
		//connect to the source node
		try {
			connectTo();
		//send the command Replicate filename
		//send the destination list
			sendCommand();
			
		//wait for the over command
			waitResponse();
		//update the replication table to reflect the file is updated	
			
			replicationTable.get(fileName).setAction("");
			
		System.out.println("Ending the connection as node replication completed : " + ipAdr );
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	public void connectionClose() throws IOException{
		srcInput.close();
		srcOutput.close();
		srcSocket.close();
	}
	
	public void connectTo() throws UnknownHostException, IOException{
		System.out.println("Connecting to " + ipAdr);
		
		srcSocket = new Socket(InetAddress.getByName(ipAdr), Constants.masterTCPPort);
		srcOutput = new DataOutputStream(srcSocket.getOutputStream());
		srcInput = new DataInputStream( srcSocket.getInputStream());
	}
	
	public void sendCommand() throws IOException{
		System.out.println("Writing Replicate Nodes to the source Node");
		srcOutput.writeUTF("ReplicateNodes " + fileName);
		System.out.println("Sending the destList");
		
		ObjectOutputStream objectOut = new ObjectOutputStream(srcSocket.getOutputStream());
		objectOut.writeObject(destList);
		
		System.out.println("Destinate node hashkey list sent, waiting for over command");
	}
	
	public void waitResponse() throws IOException{
		System.out.println("Waiting for source node to reply back");
		
		String response;
		response = srcInput.readUTF();
		
		System.out.println("The source node says " + response);
		System.out.println("Updating the replication node to reflect changes done");
	}
	
}

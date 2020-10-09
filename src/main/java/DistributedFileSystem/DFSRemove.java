package DistributedFileSystem;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

/**
 * @author Manik, Neha
 *
 * DFSRemove class performs the following functions:
 * 1. Handles end user Remove request
 * 2. Creates TCP connection with the Master Node to fetch the Node list where the file exists
 * 3. Deletes the requested file from the destination Nodes as received from the Master 
      by connecting to DFSRequestHandler
 * 4. Receives ack from the destination nodes once the file process is completed
 * Contains helper function for Socket Connection and input/output stream setup
 */

public class DFSRemove implements Runnable{
	
	//initialize socket and input stream
	private Socket dfsRmSocket = null;
	private DataInputStream input = null;
	private DataOutputStream output = null;
	Socket masterSocket = null;
	DataInputStream masterInput = null;
	DataOutputStream masterOutput = null;
	static ArrayList<String> localMasterList = new ArrayList<String>();
	int port = Constants.masterTCPPort;
	static Map<Integer, ChordNode> copyOfLocalMembTbl = new HashMap<Integer, ChordNode>();
	static ArrayList<Integer> RmNodeList = new ArrayList<Integer>();
	String dfsFileName;
	FileInputStream fis = null;
	
	public DFSRemove(String dfsFileName, ArrayList<String> masterList, Map<Integer, ChordNode> copyOfLocalMembTbl){
		this.dfsFileName = dfsFileName;
		localMasterList = masterList;
		this.copyOfLocalMembTbl = copyOfLocalMembTbl;
	}
	
	@Override
	public void run(){
		System.out.println("Removing the File from the DFS");
		try {
			boolean fileExists = connectToMaster();
			if(fileExists)
			removeFilefromNodes();
		} catch (IOException |ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		System.out.println("=====================");
		System.out.println("+                   +");
		System.out.println("+Ending user request+");
		System.out.println("+                   +");
		System.out.println("=====================");
		
	}
	
	//create TCP connection with the Master Node
	public boolean connectToMaster() throws UnknownHostException, IOException, ClassNotFoundException{
		String ack;
		boolean fileExists = false;
		System.out.println("Connecting to the Master to get the DFS Location");
		for(String ml: localMasterList){
			try {
				
				masterSocket = new Socket(InetAddress.getByName(ml), port);;
				System.out.println("Connected to the Master at : " + ml);
				
				//take input from the socket 
				masterInput = new DataInputStream(new BufferedInputStream
						(masterSocket.getInputStream()));
				
				//send output to the socket
				masterOutput = new DataOutputStream(masterSocket.getOutputStream());
				
				//connectionSetup(ml);
			}catch(IOException e) {
				System.out.println(e);	
			}
			System.out.println("Sending the Remove Location command");
			masterOutput.writeUTF("RemoveLocation " + dfsFileName);
			
			
			for(int i=0;i<10;i++){
				try{
					masterSocket.setSoTimeout(30000);
					ack = masterInput.readUTF();
					System.out.println("The master says.." + ack);
				}catch (SocketTimeoutException e ){
					break;
				}
				if(ack.equalsIgnoreCase("Wait")){
					System.out.println("The file is under updation, waiting for update to end...");
				}else{
					
						break;
					}
			}
			
		}
		masterSocket.setSoTimeout(0);;
			
			ack = masterInput.readUTF();
			System.out.println("The master says..." + ack);
			if(ack.equalsIgnoreCase("File exists")){
				fileExists = true;
				System.out.println("File exists with the destination, waiting for the file locations");
				ObjectInputStream objectInput = new ObjectInputStream(masterInput);
				RmNodeList = (ArrayList<Integer>) objectInput.readObject();
			}else{
				System.out.println("File does not exist, enter the correct file name");
				fileExists = false;
				//closing master connection if the file doesn't exists, else persist it
				masterInput.close();
				masterOutput.close();
				masterSocket.close();
				return fileExists;
			}
		System.out.println("The file exists at nodes : " + RmNodeList);
		//connectionClose();//closing the connection with the Master Node
		
		return fileExists;
	}
	
	//Function to Remove file from remote nodes
	public void removeFilefromNodes() throws UnknownHostException, IOException{
		System.out.println("The File exists in DFS at given Nodes...sending Remove command");
		ArrayList<String> ipAddressList = new ArrayList<String>(); 
		for(Integer ky: RmNodeList){
			if(copyOfLocalMembTbl.containsKey(ky)){
				ipAddressList.add(copyOfLocalMembTbl.get(ky).ipAddress);
			}
		}
		String ack;
		for(String ipAdd: ipAddressList){
			connectionSetup(ipAdd);
			System.out.println("Connected with Node at " + dfsRmSocket.getInetAddress().getCanonicalHostName());
			output.writeUTF("Remove " + dfsFileName);
			for(int i=0;i<10;i++){
				try{
					dfsRmSocket.setSoTimeout(30000);
					ack = input.readUTF();
					System.out.println("The node says.." + ack);
				}catch (SocketTimeoutException e ){
					break;
				}
				if(ack.equalsIgnoreCase("Wait")){
					System.out.println("The file is under updation, waiting for update to end...");
				}else{
					
						break;
					}
			}
			dfsRmSocket.setSoTimeout(0);
			ack = input.readUTF();
				System.out.println("The node says.." + ack);
			if(ack.equalsIgnoreCase("File exists")){
				System.out.println("File exists in the node, will be removed...");
				System.out.println(input.readUTF() + " from node " + ipAdd);
			}else{
				System.out.println("File doesn't exist in the  node");
				
			}
			
			connectionClose();
			
		}
		
		masterOutput.writeUTF("RemoveCommit " + dfsFileName);
		//Closing the master connection
		masterInput.close();
		masterOutput.close();
		masterSocket.close();
	    //close the connection once the File is written to the Remote Node
	}
	
	//Helper function for Socket connection setup
	public void connectionSetup(String ipAddress) throws UnknownHostException, IOException{
		
		dfsRmSocket = new Socket(InetAddress.getByName(ipAddress), port);
		System.out.println("Connected");
		
		//take input from the socket 
		input = new DataInputStream(new BufferedInputStream
				(dfsRmSocket.getInputStream()));
		
		//send output to the socket
		output = new DataOutputStream(dfsRmSocket.getOutputStream());	
	}
	
	//Helper function for Socket and input/output stream close
	public void connectionClose() throws IOException{
		input.close();
		output.close();
		dfsRmSocket.close();
	}
}

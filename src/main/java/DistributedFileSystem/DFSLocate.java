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
 * DFSLocate class performs the following functions:
 * 1. Handles end user Locate request
 * 2. Creates TCP connection with the Master Node to fetch the Node list
 * 3. Locate the file at the Nodes as received from the Master by connecting to DFSRequestHandler
 * Contains helper function for Socket Connection and input/output stream setup
 */

public class DFSLocate implements Runnable {
	
	//initialize socket and input stream
	private Socket masterSocket = null;
	private DataInputStream masterInput = null;
	private DataOutputStream masterOutput = null;
	ArrayList<String> localMasterList = new ArrayList<String>();
	int port = Constants.masterTCPPort;
	Map<Integer, ChordNode> copyOfLocalMembTbl = new HashMap<Integer, ChordNode>();
	ArrayList<Integer> GetNodeList = new ArrayList<Integer>();
	String dfsFileName;
	FileInputStream fis = null;
	
	public DFSLocate(String dfsFileName, ArrayList<String> masterList, Map<Integer, ChordNode> copyOfLocalMembTbl){
		this.dfsFileName = dfsFileName;
		localMasterList = masterList;
		this.copyOfLocalMembTbl = copyOfLocalMembTbl;
	}

	@Override
	public void run(){
		try {
			connectToMaster();
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
	public void connectToMaster() throws UnknownHostException, IOException, ClassNotFoundException{
		
		for(String ml: localMasterList){
			try {
				connectionSetup(ml);
			}catch(IOException e) {
				System.out.println(e);	
			}
			String ack;
			System.out.println("Sending Locate command to the Master");
			masterOutput.writeUTF("Locate " + dfsFileName);
			
			
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
			ack = masterInput.readUTF();
			System.out.println("The master says.." + ack);
			if(ack.equalsIgnoreCase("File exists")){
				System.out.println("File exists with the destination, waiting for the file locations");
				ObjectInputStream objectInput = new ObjectInputStream(masterInput);
				GetNodeList = (ArrayList<Integer>) objectInput.readObject();
				connectionClose();//closing the connection with the Master Node
				break;
				
			}else{
				System.out.println("File does not exist, enter the correct file name");
				connectionClose();//closing the connection with the Master Node
				break;
			}
		}
		
		if(!GetNodeList.isEmpty())
			System.out.println("IP address of the nodes where the file exist::" +GetNodeList);
	}
	
	//Helper function for Socket connection setup
	public void connectionSetup(String ipAddress) throws UnknownHostException, IOException{
		
		masterSocket = new Socket(InetAddress.getByName(ipAddress), port);
		System.out.println("Connected to master at : " + ipAddress);
		
		//take input from the socket 
		masterInput = new DataInputStream(masterSocket.getInputStream());
		
		//send output to the socket
		masterOutput = new DataOutputStream(masterSocket.getOutputStream());	
	}
	
	//Helper function for Socket and input/output stream close
	public void connectionClose() throws IOException{
		masterInput.close();
		masterOutput.close();
		masterSocket.close();
	}
}

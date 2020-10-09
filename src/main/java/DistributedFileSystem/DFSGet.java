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
 * DFSGet class performs the following functions:
 * 1. Handles end user Get request
 * 2. Creates TCP connection with the Master Node to fetch the Node list
 * 3. Gets the requested file from the Nodes as received from the Master by connecting to DFSRequestHandler
 * 4. Writes requested file to the Remote Node
 * Contains helper function for Socket Connection and input/output stream setup
 */

public class DFSGet  implements Runnable{
	//initialize socket and input stream
	private Socket dfsGetSocket = null;
	private static DataInputStream input = null;
	private static DataOutputStream output = null;
	private Socket masterSocket = null;
	private DataInputStream masterInput = null;
	private DataOutputStream masterOutput = null;
	ArrayList<String> localMasterList = new ArrayList<String>();
	int port = Constants.masterTCPPort;
	static Map<Integer, ChordNode> copyOfLocalMembTbl = new HashMap<Integer, ChordNode>();
	static ArrayList<Integer> GetNodeList = new ArrayList<Integer>();
	static String localFileName, dfsFileName;
	FileOutputStream fos = null;
	
	public DFSGet(String localFileName,String dfsFileName, ArrayList<String> masterList, Map<Integer, ChordNode> copyOfLocalMembTbl){
		this.localFileName = localFileName;
		this.dfsFileName = dfsFileName;
		localMasterList = masterList;
		this.copyOfLocalMembTbl = copyOfLocalMembTbl;
	}
	
	@Override
	public void run(){
		
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "=======================");
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "+                     +");
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "+Starting user request+");
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "+                     +");
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "=======================");
		
		long start = System.currentTimeMillis();
		try {
			connectToMaster();
			if(GetNodeList!=null){
				getFilefromNodes();
			}else{
				System.out.println("File doesn't exist");
			}
			System.out.println("Sending over command to the master " + dfsFileName);
			masterOutput.writeUTF("GetOver " + dfsFileName);
			
			try{
				if(masterOutput!=null) masterOutput.close();
				if(masterInput!=null) masterInput.close();
				masterSocket.close();
			}catch (IOException e){}
		} catch (IOException |ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		long timetaken = end - start;
		
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "The time taken is : " + timetaken);
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "=====================");
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "+                   +");
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "+Ending user request+");
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "+                   +");
		System.out.println("["+DFSGet.this.getClass().getSimpleName() + "] : " + "=====================");
		
		
	}
	
	//create TCP connection with the Master Node
	public void connectToMaster() throws UnknownHostException, IOException, ClassNotFoundException{
		System.out.println("["+ System.currentTimeMillis() + ":" + DFSGet.this.getClass().getSimpleName() + "] : " + "Connecting to the Master to get the File Location");
		for(String ml: localMasterList){
			try {
				System.out.println("Connecting to master at : " + ml);
				masterSocket = new Socket(InetAddress.getByName(ml), port); 
				masterInput = new DataInputStream(masterSocket.getInputStream());
				masterOutput = new DataOutputStream(masterSocket.getOutputStream());
				System.out.println(masterSocket.isConnected());
				//connectionSetup(ml);
			}catch(IOException e) {
				System.out.println(e);	
			}
			System.out.println("The filename is : " + dfsFileName);
			masterOutput.writeUTF("GetLocation " + dfsFileName);
			String ack;
			
			for(int i=0;i<10;i++){
				try{
					masterSocket.setSoTimeout(30000);
					ack = masterInput.readUTF();
					System.out.println("the response is : " + ack);
				}catch (SocketTimeoutException e ){
					break;
				}
				if(ack.equalsIgnoreCase("Wait")){
					System.out.println("The file is under updation, waiting for update to end...");
				}else{
					break;
				}
			}
			masterSocket.setSoTimeout(0);
			ack = masterInput.readUTF();
			System.out.println("The master says " + ack);
			if(ack.equalsIgnoreCase("File Exists")){
				System.out.println("["+ System.currentTimeMillis() + ":" + DFSGet.this.getClass().getSimpleName() + "] : " + "File exists with the Master, waiting for the Node List");
				ObjectInputStream objectInput = new ObjectInputStream(masterInput);
				GetNodeList = (ArrayList<Integer>) objectInput.readObject();
				System.out.println("["+ System.currentTimeMillis() + ":" + DFSGet.this.getClass().getSimpleName() + "] : " + "The DFS Node List is : " + GetNodeList);
				//objectInput.close();
				
				break;
			}else{
				System.out.println("Closing connection with the master");
				masterInput.close();
				masterOutput.close();
				masterSocket.close();
			}
			System.out.println("Ending the fetch table from Master");
			
		}
		//connectionClose();//closing the connection with the Master Node
	}
	
	public void getFilefromNodes()throws IOException, InterruptedException{
		System.out.println("["+ System.currentTimeMillis() + ":" + DFSGet.this.getClass().getSimpleName() + "] : " + "Fetching the file list");
		ArrayList<String> ipAddressList = new ArrayList<String>(); 
		
		String response;
		for(Integer ky: GetNodeList){
			if(copyOfLocalMembTbl.containsKey(ky)){
				ipAddressList.add(copyOfLocalMembTbl.get(ky).ipAddress);
			}
		}
		System.out.println("["+ System.currentTimeMillis() + ":" + DFSGet.this.getClass().getSimpleName() + "] : " + "The Source Node IP Address is : " + ipAddressList);
		for(String ipAdd: ipAddressList){
			connectionSetup(ipAdd);
			System.out.println("Connect with : " + dfsGetSocket.getInetAddress());
			System.out.println("Sending Get for " + dfsFileName);
			output.writeUTF("Get " + dfsFileName);
			for(int i =0; i<10;i++){
				response = input.readUTF();
				System.out.println("The response is : " + response);
				
					if(response.equalsIgnoreCase("Wait")){
						Thread.sleep(30000);
					}else{
						break;
					}
			}
			
			response = input.readUTF();
			System.out.println("The response is : " + response);
			if(response.equalsIgnoreCase("File exists")){
				String userDir = System.getProperty("user.home");
				String fileDir = userDir + File.separator + "LocalFiles" + File.separator;
				System.out.println(fileDir);
				File file = new File(fileDir + localFileName);
					if(file.exists()){
						file.delete();
						System.out.println("Deleting older copy");
					}
				file.createNewFile();
				fos = new FileOutputStream(file);
				IOUtils.copyLarge(input, fos);
				fos.flush();
				System.out.println("["+ System.currentTimeMillis() + ":" + DFSGet.this.getClass().getSimpleName() + "] : " + "File downloaded from the Source Node");
				//System.out.println("The node at " + ipAdd + " says " + input.readUTF());
				System.out.println("Terminating connection with the node");
				
				output.writeUTF("GetOver " + dfsFileName);
				connectionClose();
				
				return;
			}else{
				System.out.println("File doesn't exist in node " + ipAdd);
				connectionClose();
			}
			//close the connection once the File is written to the Remote Node
		}
	}
	
	//Helper function for Socket connection setup
	public void connectionSetup(String ipAddress) throws UnknownHostException, IOException{
		
		dfsGetSocket = new Socket(InetAddress.getByName(ipAddress), port);
		System.out.println("Connected");
		
		//take input from the socket 
		input = new DataInputStream(new BufferedInputStream
				(dfsGetSocket.getInputStream()));
		
		//send output to the socket
		output = new DataOutputStream(dfsGetSocket.getOutputStream());	
	}
	
	//Helper function for Socket and input/output stream close
	public void connectionClose() throws IOException{
		input.close();
		output.close();
		dfsGetSocket.close();
	}

}

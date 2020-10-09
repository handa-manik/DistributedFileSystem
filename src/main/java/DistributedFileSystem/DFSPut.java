package DistributedFileSystem;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;

/**
 * @author Manik, Neha
 *
 * DFSPut class performs the following functions:
 * 1. Handles end user Put request
 * 2. Creates TCP connection with the Master Node to fetch the Node list
 * 3. Puts the requested file at the destination Nodes as received from the Master 
      by connecting to DFSRequestHandler
 * 4. Receives ack from the destination nodes once the file process is completed
 * Contains helper function for Socket Connection and input/output stream setup
 */

public class DFSPut implements Runnable {
	
	//initialize socket and input stream
	private Socket dfsPutSocket = null;
	public static  Socket masterSocket = null;
	private DataInputStream input = null;
	private DataOutputStream output = null;
	public static  DataInputStream masterInput = null;
	public static  DataOutputStream masterOut = null;
	ArrayList<String> localMasterList = new ArrayList<String>();
	int port = Constants.masterTCPPort;
	static Map<Integer, ChordNode> copyOfLocalMembTbl = new HashMap<Integer, ChordNode>();
	static ArrayList<Integer> putNodeList = new ArrayList<Integer>();
	String localFileName, dfsFileName;
	FileInputStream fis = null;
	static boolean FileExists = false;
	String ipArray = "";
	static HashMap<String, DFSTableEntry> fileLocationMap2 = new HashMap<String, DFSTableEntry>(); 
	boolean isReplicate = false;
	
	public DFSPut(String localFileName, String dfsFileName, ArrayList<String> masterList, Map<Integer, ChordNode> copyOfLocalMembTbl){
		this.localFileName = localFileName;
		this.dfsFileName = dfsFileName;
		localMasterList = masterList;
		this.copyOfLocalMembTbl = copyOfLocalMembTbl;
	}
	
	public DFSPut(String dfsFileName, ArrayList<Integer> hklist, Map<Integer, ChordNode> copyOfLocalMembTbl, HashMap<String, DFSTableEntry> fileLocationMap2){
		this.dfsFileName = dfsFileName;
		this.copyOfLocalMembTbl = copyOfLocalMembTbl;
		this.fileLocationMap2 = fileLocationMap2;
		this.isReplicate = true;
		this.putNodeList = hklist;
		this.isReplicate = true;
	}
	
	@Override
	public void run(){
		
		System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "=======================");
		System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "+                     +");
		System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "+Starting user request+");
		System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "+                     +");
		System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "=======================");
		
		long start = System.currentTimeMillis();
		

		
		
		
		try {
			if(!isReplicate){
				connectToMaster();
				putFiletoNodes();
				masterInput.close();
				masterOut.close();
				masterSocket.close();
			}else{
				System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "Starting file replication");
				putFiletoNodes();
			}
			
			
			long end = System.currentTimeMillis();
			long timetaken = end - start;
			
			System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "The time taken is : " + timetaken);
			System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "=====================");
			System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "+                   +");
			System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "+Ending user request+");
			System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "+                   +");
			System.out.println("["+DFSPut.this.getClass().getSimpleName() + "] : " + "=====================");
			
						
		} catch (IOException |ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	//create TCP connection with the Master Node
	public void connectToMaster() throws UnknownHostException, IOException, ClassNotFoundException{
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSPut.this.getClass().getSimpleName() + "] : " + "Connecting to Master to check if the file exists");
		FileExists = false;
		for(String ml: localMasterList){
			try {
				masterSocket = connectionSetup(ml);
				System.out.println("Connected to master at : " + ml);
			}catch(IOException e) {
				System.out.println(e);	
			}
			masterOut = new DataOutputStream(masterSocket.getOutputStream());
			masterInput = new DataInputStream(masterSocket.getInputStream());
			System.out.println("Sending command PutLocation" + dfsFileName);
			masterOut.writeUTF("PutLocation " + dfsFileName);
			
			String ack; 
			
			
			
			for(int i=0; i<10 ; i++){
				try{
					masterSocket.setSoTimeout(30000);
					System.out.println("Waiting for confirmation from Master about queue");
					ack = masterInput.readUTF();
					System.out.println("The master says..." + ack);
				}catch (SocketTimeoutException e ){
					break;
				}
				masterSocket.setSoTimeout(0);
				if(ack.equalsIgnoreCase("Wait")){
					System.out.println("The file is under updation, waiting for update to end...");
				}else{
					System.out.println("The file is not being updated...progressing ahead and receiving the destination node list");
					ack = masterInput.readUTF();
					System.out.println("The Master says..." + ack);
					ObjectInputStream objectInput = new ObjectInputStream(masterInput);
					putNodeList = (ArrayList<Integer>) objectInput.readObject();
					System.out.println("["+ System.currentTimeMillis() + ":" +DFSPut.this.getClass().getSimpleName() + "] : " + "The Destination Node list is : " + putNodeList);
					//objectInput.close();
					
					return;
					
				}
			}
		
		}
	}
	
	//put file to the nodes as selected by the Master Node
	public void putFiletoNodes() throws UnknownHostException, IOException, InterruptedException{
		
		ArrayList<String> ipAddressList = new ArrayList<String>();
		ArrayList<Socket> nodeSockets = new ArrayList<Socket>();
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSPut.this.getClass().getSimpleName() + "] : " + "The Local Mem Table is : " + copyOfLocalMembTbl.keySet());
		
		for(Integer ky: putNodeList){
			System.out.println("The Node Hashkey is : " + ky);
			if(copyOfLocalMembTbl.containsKey(ky)){
				ipAddressList.add(copyOfLocalMembTbl.get(ky).ipAddress);
			}
		}
		Socket tempSocket = null;
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSPut.this.getClass().getSimpleName() + "] : " + "The file will be sent to the Nodes with the following IP Address : " + ipAddressList);
		for(int j=0;j<ipAddressList.size();j++){
			String ipAdd = ipAddressList.get(j);
			System.out.println("Creating Socket with " + ipAdd);
			String response;
			
			if(ipAdd.equals(Constants.GatewayNodeAddress))
			{	System.out.println("This is a master node, connection exists");
				
				System.out.println(masterSocket.isClosed());
				masterOut.writeUTF("Put " + dfsFileName);
				
				masterSocket.setSoTimeout(0);
				response = masterInput.readUTF();
					
				System.out.println("Response received is : " + response);
				response = masterInput.readUTF();
				
				System.out.println("Response received is : " + response);
				
				if(response.equals("Ready"))
				{nodeSockets.add(masterSocket); ipArray = ipArray + ipAdd + ";";}
				else{ipAddressList.remove(ipAddressList.indexOf(ipAdd));}
			}
			else
				{tempSocket= connectionSetup(ipAdd);
				System.out.println("Sending put");
				output = new DataOutputStream(tempSocket.getOutputStream());
				input = new DataInputStream(tempSocket.getInputStream());
				System.out.println("Put Send");
				output.writeUTF("Put " + dfsFileName);
				for(int i=0;i<10;i++){
					response = input.readUTF();
						if(response.equalsIgnoreCase("Wait")){Thread.sleep(30000);}
						else{
							
							System.out.println("The node says " + response);
							break;
						}
				}
				
				response = input.readUTF();
				//File exists or not response
				System.out.println(response);
				
				
				//output.writeUTF("Put " + dfsFileName);
				tempSocket.setSoTimeout(10000);
				response = input.readUTF();
				tempSocket.setSoTimeout(0);
				
				System.out.println("Response received is : " + response);
				if(response.equals("Ready"))
					{nodeSockets.add(tempSocket); ipArray = ipArray + ipAdd + ";";}
				else{ipAddressList.remove(ipAddressList.indexOf(ipAdd));}
				tempSocket = null;
				}
		}
		System.out.println("The File will be sent to : " + ipAddressList);
		if(ipAddressList.size()>=Constants.VC){
			//ExecutorService executor = Executors.newFixedThreadPool(1);
			String userDir = System.getProperty("user.home");
			String dir = "";
			String fileDir = "";
			if(isReplicate){
				dir = "DFSFileSystem";
				localFileName = dfsFileName;
			}else{dir = "LocalFiles";}
			fileDir = userDir + File.separator + dir + File.separator;
			
			File file = new File(fileDir + localFileName);
			System.out.println("File found in the local directory");
		
			for(Socket tSock : nodeSockets){
				
				fis = new FileInputStream(file);
				DataOutputStream tOut = new DataOutputStream(tSock.getOutputStream());
				DataInputStream tIn = new DataInputStream(tSock.getInputStream());
				System.out.println("Starting File Transmission for address : " + tSock.getInetAddress().toString());
				IOUtils.copyLarge(fis, tOut);
				
				System.out.println("In the clear");
				
				if(tSock.getInetAddress() != masterSocket.getInetAddress())
				{System.out.println("Closing for " + tSock.getInetAddress().toString());
					tIn.close();tOut.close();tSock.close();
				}else{
					System.out.println(tSock.getInetAddress().getHostAddress());
				}
			}
			
			nodeSockets.clear();
			System.out.println("All Files written");
		}
			
		if(isReplicate){
			System.out.println("Ending the Put instruction");
		}else{
			String hkList = "";
			String[] hkArr ;
			hkArr = ipArray.split(";");
			for(Integer hk: putNodeList){
				for(int i=0;i<hkArr.length;i++){
					if(copyOfLocalMembTbl.get(hk).ipAddress.equalsIgnoreCase(hkArr[i])){
						System.out.println("The ipAddress " + hkArr[i] + " has the hashkey " + hk);
						hkList = hkList + String.valueOf(hk) + ";";
						break;
					}
				}
			}
			System.out.println("The Hash Key list of received node is : " + hkList);
			
			String tempOut = "PutCommit " + dfsFileName + " " + hkList;
			System.out.println("Sending the PutCommit to the Master : " + tempOut);
			masterOut.writeUTF(tempOut);
		
			System.out.println("Closing master connection");
			
		}
		
		
	}
	
	//Helper function for Socket connection setup
	public Socket connectionSetup(String ipAddress) throws UnknownHostException, IOException{
		Socket tempSocket;
		tempSocket = new Socket(InetAddress.getByName(ipAddress), port);
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSPut.this.getClass().getSimpleName() + "] : " + "Connected");
		
		return tempSocket;
	}
	
	//Helper function for Socket and input/output stream close
	public void connectionClose() throws IOException{
		input.close();
		output.close();
		dfsPutSocket.close();
	}
}

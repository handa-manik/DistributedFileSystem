package DistributedFileSystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;

/**
 * @author Manik, Neha
 *
 * DFSRequestThread class performs the following functions:
 * 1. Handles the request from the user 
 * 2. Invokes the requested action and returns the data requested
 * 3. Invokes the DFSRequestHandler thread to complete the file request
 * Contains helper function for Socket Connection and input/output stream setup
 */

public class DFSRequestThread implements Runnable{
	
	ArrayList<String> masterList = new ArrayList<String>();
	private File localFileName;
	static Map<Integer, ChordNode> copyOfLocalMembTbl = new HashMap<Integer, ChordNode>();
	private ServerSocket DFSServerSocket = null;
	private Socket DFSSocket = null;
	private DataInputStream input ;;
	private DataOutputStream output ;
	public boolean isMaster = false;
	private int generalDFSPort = Constants.masterTCPPort;
	// DFSTableEntry dfst = new DFSTableEntry();
	static HashMap<String, DFSTableEntry> fileLocationMap2 = new HashMap<String, DFSTableEntry>(); 
		static HashMap<String, ArrayList<Integer>> fileLocationMap = new HashMap<String, ArrayList<Integer>>(); 
	static HashMap<String, Long> localFileListMap = new HashMap<String, Long>();
	
	public DFSRequestThread(Socket DFSSocket, Map<Integer, ChordNode> copyOfLocalMembTbl, HashMap<String, ArrayList<Integer>> fileLocationMap, HashMap<String, DFSTableEntry> fileLocationMap2) throws IOException{
		System.out.println("Starting DFS Thread for " + DFSSocket.getInetAddress().toString());
		this.fileLocationMap2 = fileLocationMap2;
		this.fileLocationMap = fileLocationMap;
		this.copyOfLocalMembTbl = copyOfLocalMembTbl;
		this.DFSSocket = DFSSocket;

		if(copyOfLocalMembTbl.get(ChordNodeMain.node.hashKey).isMaster){
			isMaster = true;
		}
		
		input = new DataInputStream(this.DFSSocket.getInputStream());
		output = new DataOutputStream(this.DFSSocket.getOutputStream());
}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSRequestThread.this.getClass().getSimpleName() + "] : " 
		+ "Starting the DFS Request Thread");
		System.out.println("The current file Location table is .. " + fileLocationMap2.keySet());
		try {
			receiveNodeRequests();
		} catch (IOException | InterruptedException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println("=====================");
		System.out.println("+                   +");
		System.out.println("+Ending user request+");
		System.out.println("+                   +");
		System.out.println("=====================");
		
	}
	
	
	public void receiveNodeRequests() throws IOException, InterruptedException, ClassNotFoundException{

		String userInput = null;
		int requestNodeKey = 0;
		String requestNodeAdd = null;
		boolean fileExists = false;
		
		try{
			DFSSocket.setSoTimeout(0);
			userInput = input.readUTF();
		}catch (EOFException e){
			return;
		}
		System.out.println("User input is " + userInput);
		String []command = userInput.split(" ");
		
		if((command.length==3) && command[0].equalsIgnoreCase("Put")){
			command[1] = command[2];
		}
		
		if(command.length>1){
			if(fileLocationMap2.containsKey(command[1])){
				for(int i=0;i<10;i++){
					if(fileLocationMap2.get(command[1]).getAction().equalsIgnoreCase("Writing")){
						System.out.println("Sending wait..");
						output.writeUTF("Wait");
						Thread.sleep(30000);
					}else{
						System.out.println("Sending In Progress...");
						output.writeUTF("In Progress");
						break;
					}	
				}
			}else{
				output.writeUTF("In Progress");
			}	
		}
			
		int c=0;
		
		while(true){
			System.out.println("["+ System.currentTimeMillis() + ":" +DFSRequestThread.this.getClass().getSimpleName() + "] : "
		+ "Waiting for new DFS Request");

			if(c!=0){
				System.out.println("Waiting for user input");
				DFSSocket.setSoTimeout(0);
				try{userInput = input.readUTF();}
				catch(EOFException e){
					return;
				}
				System.out.println("User input is " + userInput);
				command = userInput.split(" ");
			}
			c++;
			requestNodeAdd = DFSSocket.getInetAddress().toString().replace("/", "");
			System.out.println("The Request Node address is : " + requestNodeAdd);
			for(ChordNode ch: copyOfLocalMembTbl.values()){
				System.out.println("The node address is : " + ch.ipAddress);
				if(ch.ipAddress.contains(requestNodeAdd))
					requestNodeKey = ch.hashKey;
			}			
			System.out.println("The received command is " + command[0] + " from the Request Node: " + requestNodeKey);			
			switch (command[0]){
			
				case "PutLocation": //as received by the Master node to enquire about the destination nodes
					//Check if the file exists in the DFS and abort if it does
					fileExists = checkIfFileExists(command[0],command[1]);
					
						handleNodesLocRequest(command[0],command[1], requestNodeKey, fileExists);
					
					break;
					
				case "Put":
					//as received by the destination node to store the file content in a new file
					fileExists = checkIfFileExists(command[0],command[1]);
					handlePutRequest(command[1], fileExists);
					if(!isMaster){command[0] = "PutCommit";}
					
					break;
					
				case "GetLocation":
					//as received by the master node get the location of nodes where file exists
					fileExists = checkIfFileExists(command[0], command[1]);
					if(fileExists)handleNodesLocRequest(command[0],command[1], requestNodeKey, fileExists);
					else{command[0] = "Over";}
					
					break;
					
				case "Get":
					//get the file and send it to the requested node
					fileExists = checkIfFileExists(command[0], command[1]);
					if(fileExists)handleGetRequest(command[1], fileExists);
					else{command[0] = "Over";}
					break;
					
				case "RemoveLocation":
					//get the node list from where the file is to be removed
					fileExists = checkIfFileExists(command[0], command[1]);
					if(fileExists){handleNodesLocRequest(command[0],command[1], requestNodeKey, fileExists);}
					else{command[0] = "Over";}
					break;
					
				case "Remove":
					//Includes logic to remove file from the master and the remote node
					fileExists = checkIfFileExists(command[0], command[1]);
					if(fileExists)	handleRemoveRequest(command[1]);
					command[0] = "Over";
					break;
				
				case "ls":
					//Get all the files that are in the file system from the master
					handlelsRequest();
					command[0] = "Over";
					break;
					
				case "Locate":
					//Get the Ip Addresses of the nodes where the requested file has been stored
					fileExists = checkIfFileExists(command[0], command [1]);
					if(fileExists){handleNodesLocRequest(command[0], command[1], requestNodeKey,fileExists );}
					else{ command[0] = "Over";}
					command[0] = "Over";
					break;
					
				case "PutCommit":
					//Commit confirmation received from User, Updating the master file table
					String nodeArray[] = command[2].split(";");
					ArrayList<Integer> tempHK = new ArrayList<Integer>();
					for(int i =0; i<nodeArray.length; i++){tempHK.add(Integer.parseInt(nodeArray[i]));}
					DFSTableEntry dft = new DFSTableEntry();
					System.out.println("Setting the hashkey for " + command[1] + " as " + tempHK);
					dft.setHashKeyList(tempHK);
					fileLocationMap2.put(command[1], dft);
					System.out.println("The file table updated is " + fileLocationMap2.keySet());
					
					break;
				
				case "updateMastertable":
					//wait for the table to come and then Update the Master Table
					System.out.println("Connected for Master Table Update");
					handleMasterUpdate();
					System.out.println("The master file table has been updated");
					break;
					
				
				case "RemoveCommit":
					System.out.println("Removal complete from the name nodes");
					fileLocationMap2.remove(command[1]);
					break;
					
				case "GetOver":
					System.out.println("File transfer complete, unlocking the file " + command[1]);
					fileLocationMap2.get(command[1]).setAction("");
					command[0] = "Over";
				break;
				
				case "ReplicateNodes":
					//command[1] has the filename
					//start the dfs put here
					fileLocationMap2.get(command[1]).setAction("Writing");
					replicateNode(command[1]);
					System.out.println("Replication completed");
					command[0] = "Over";
					break;
				
					
					
			}
			
			
			
			if(command[0].equalsIgnoreCase("PutCommit") 
				|| command[0].equalsIgnoreCase("updateMastertable")
				|| command[0].equalsIgnoreCase("ls") 
				
				|| command[0].equalsIgnoreCase("RemoveCommit")
				|| command[0].equalsIgnoreCase("Over")
				){
				System.out.println("Closing all connections");
				
				if(DFSSocket.isConnected()){
					input.close();
					output.close();
					DFSSocket.close();
					return;
				}
				
			}
			
		}
	}
	
	private void replicateNode(String dfsFilename) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		ArrayList<Integer> temp_destList = new ArrayList<Integer>();
		//wait for the hklist to come
		ObjectInputStream objectInput = new ObjectInputStream(input);
		temp_destList = (ArrayList<Integer>) objectInput.readObject();
		//start the dfs put
		DFSPut repPut = new DFSPut(dfsFilename, temp_destList, copyOfLocalMembTbl, fileLocationMap2);
		Thread RepPut = new Thread(repPut);
		System.out.println("Starting the replication thread");
		RepPut.start();
		while(RepPut.getState().equals(Thread.State.RUNNABLE)){
			System.out.println("The Thread to replicate the file to destination is still running");
			Thread.sleep(30000);
		}
		
		System.out.println("Informing the source node about the replication completion");
		output.writeUTF("Over");
	}

	public void handleMasterUpdate() throws IOException, ClassNotFoundException{
		//wait for the map to come and then parse it to update the MasterList
		HashMap<String, ArrayList<Integer>> temp_FileMap = new HashMap<String, ArrayList<Integer>>();
		ArrayList<Integer> hk_list = new ArrayList<Integer>();
		ObjectInputStream objectInput = new ObjectInputStream(input);
		temp_FileMap = (HashMap<String, ArrayList<Integer>>) objectInput.readObject();

		for(Entry<String, ArrayList<Integer>> pair : temp_FileMap.entrySet()){
			System.out.println("Checking entry for ... " + pair.getKey());
			if(fileLocationMap.containsKey(pair.getKey())&& fileLocationMap.get(pair.getKey()).contains(pair.getValue())!=true){
				hk_list.clear();
				hk_list.addAll(fileLocationMap.get(pair.getKey()));
				System.out.println(hk_list);
				hk_list.addAll(pair.getValue());
				System.out.println(hk_list);
				fileLocationMap.put(pair.getKey(), hk_list);
				System.out.println("Stored entry for : " + pair.getKey() + " a list : " + hk_list);
			}else{
				fileLocationMap.put(pair.getKey(), pair.getValue());
				System.out.println("No match found in the fileLocationMap");
			}
		}
		//updating master now!
		System.out.println("Merging with the main master now....");
		//ChordNodeAction.dfs.updateMasterFileTable(fileLocationMap);
		
	}
	
	public boolean checkIfFileExists(String requestType, String cmdFileName) throws IOException{
		if(fileLocationMap2.containsKey(cmdFileName)){
			System.out.println("["+ System.currentTimeMillis() + ":" +DFSRequestThread.this.getClass().getSimpleName() + "] : " + "File already exists in the DFS, informing the user");
			output.writeUTF("File exists");
			return true;
		}else{
			if(requestType.equalsIgnoreCase("PutLocation")){
				System.out.println("["+ System.currentTimeMillis() + ":" +DFSRequestThread.this.getClass().getSimpleName() + "] : " + "File doesn't exist in DFS, providing the user with Destination Node list");
				output.writeUTF("Sending Destination Node List");
				System.out.println("Node list msg sent");
			}else if(requestType.equalsIgnoreCase("GetLocation")
				     || requestType.equalsIgnoreCase("Locate")
				     || requestType.equalsIgnoreCase("RemoveLocation")
				     || requestType.equalsIgnoreCase("Put")){
				output.writeUTF("File does not Exist");}
			else{}
		}
		return false;
	}
	
	//iterate over the membership list keyset and choose 3 random machines for replication
	public void handleNodesLocRequest(String requestType, String cmdFileName, int requestNodeKey, boolean fileExists) throws IOException{
		
		ArrayList<Integer> hashKeyList = new ArrayList<Integer>();
		hashKeyList = new ArrayList<Integer>(copyOfLocalMembTbl.keySet());
		Random random = new Random();
		Set<Integer> fileLocSet = new HashSet<Integer>();
		
		hashKeyList.remove(hashKeyList.indexOf(requestNodeKey));
		hashKeyList.remove(hashKeyList.indexOf(ChordNodeMain.node.hashKey));
		System.out.println("The hashKeyList is : " + hashKeyList);
		//fetch Nodes for locating/reading the files based on the requestType from the user
		if(requestType.equalsIgnoreCase("PutLocation") && fileExists!=true){
			System.out.println("The file doesn't exist in the DFS, creating node list");
			while(fileLocSet.size()<Constants.Rep_Factor){
				int tempKey = hashKeyList.get(random.nextInt(hashKeyList.size()));
				if(requestNodeKey != tempKey)
					{fileLocSet.add(tempKey);}
			}
			DFSTableEntry dfst = new DFSTableEntry();
			dfst.setHashKeyList(new ArrayList<Integer>(fileLocSet));
			dfst.setAction("Writing");
			dfst.setIp(DFSSocket.getInetAddress());
			fileLocationMap2.put(cmdFileName, dfst);
			
			//updateMasterFileList(fileLocSet, cmdFileName);
		}else if(requestType.equalsIgnoreCase("PutLocation") && fileExists==true){
			fileLocSet.addAll(fileLocationMap2.get(cmdFileName).getHashKeyList());
			if(fileLocSet.size()<Constants.Rep_Factor){
				while(fileLocSet.size()<3){
					int tempKey = hashKeyList.get(random.nextInt(hashKeyList.size()));
					if(requestNodeKey != tempKey)
						{fileLocSet.add(tempKey);}
				}
			}
				DFSTableEntry dfst = new DFSTableEntry();
				dfst.setHashKeyList(new ArrayList<Integer>(fileLocSet));
				dfst.setAction("Writing");
				dfst.setIp(DFSSocket.getInetAddress());
				fileLocationMap2.put(cmdFileName, dfst);
			
		}
		else if((requestType.equalsIgnoreCase("GetLocation") || requestType.equalsIgnoreCase("Locate")) && fileExists==true){
			fileLocSet.addAll(fileLocationMap2.get(cmdFileName).getHashKeyList());
			if(requestType.equalsIgnoreCase("GetLocation")){fileLocationMap2.get(cmdFileName).setAction("Writing");}
		}else if((requestType.equalsIgnoreCase("GetLocation") || requestType.equalsIgnoreCase("Locate")) && fileExists!=true){
			fileLocSet.add(0);
		}
		
		else if(requestType.equalsIgnoreCase("RemoveLocation")){
			fileLocationMap2.get(cmdFileName).setAction("Writing");
			fileLocSet.addAll(fileLocationMap2.get(cmdFileName).getHashKeyList());
			//fileLocationMap.remove(cmdFileName);
		}
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSRequestThread.this.getClass().getSimpleName() + "] : " + "The Destination Nodes are : " + fileLocSet );
		//Output the set of 3 destination machines
		ObjectOutputStream objectOut = new ObjectOutputStream(DFSSocket.getOutputStream());
		objectOut.writeObject(new ArrayList<Integer>(fileLocSet));
		objectOut.flush();
		
		
		
	}
	
	public void handlePutRequest(String cmdFileName, boolean fileExists) throws IOException{
		
		if(fileExists){
			fileLocationMap2.get(cmdFileName).setAction("Writing");
			fileLocationMap2.get(cmdFileName).setIp(DFSSocket.getInetAddress());
		}else{
			
		}
		String userDir = System.getProperty("user.home");
		String fileDir = userDir + File.separator + "DFSFileSystem" + File.separator;
		String tempFileName = "temp" + System.currentTimeMillis() + cmdFileName;
		System.out.println(fileDir+tempFileName);
		File newFile = new File(fileDir + tempFileName);
		newFile.createNewFile();
		FileOutputStream fos = new FileOutputStream(fileDir +tempFileName);
		System.out.println("Temporary file created");
		output.writeUTF("Ready");
		
		int count;
		byte[] buffer = new byte[4096];
		System.out.println("I am now waiting");
		IOUtils.copy(input, fos);

		System.out.println("["+ System.currentTimeMillis() + ":" +DFSRequestThread.this.getClass().getSimpleName() + "] : " + "File writing completed");
		//output.writeUTF("Completed");
		fos.close();
		
		System.out.println("Temp file deleted");
		if(fileExists){
			File oldFile = new File(fileDir + cmdFileName);
			oldFile.delete();
			System.out.println("Older version deleted");
		}
		File renameFile = new File(fileDir + cmdFileName);
		newFile.renameTo(renameFile);
			
		
		DFSTableEntry dfst = new DFSTableEntry();
		dfst.setHashKeyList(ChordNodeMain.node.hashKey);
		fileLocationMap2.put(cmdFileName, dfst);
		//output.writeUTF("Completed");
		System.out.println("The updated file table is " + fileLocationMap2.keySet());
		
	}
	
	public void handleGetRequest(String cmdFileName, boolean fileExists) throws IOException{
		if(fileExists){
			fileLocationMap2.get(cmdFileName).setAction("Writing");
		}
		//Writing for get command on name node
		String userDir = System.getProperty("user.home");
		String fileDir = userDir + File.separator + "DFSFileSystem" + File.separator;
		File exFile = new File(fileDir +cmdFileName);
		FileInputStream fis = new FileInputStream(exFile);
		System.out.println(exFile.getCanonicalPath());
		IOUtils.copy(fis, output);
		output.flush();
		fis.close();
		output.writeUTF("Over");
		System.out.println("File transfer complete");
	}
	
	//Remove the file and update the Local File List to reflect the changes
	public void handleRemoveRequest(String cmdFileName) throws IOException{
		String userDir = System.getProperty("user.home");
		String fileDir = userDir + File.separator + "DFSFileSystem" + File.separator;
		File file = new File(fileDir+cmdFileName);
		file.delete();
		output.writeUTF("File Deleted");
		fileLocationMap2.remove(cmdFileName);
	}
	
	//Get all file name that exist in the file system from the master
	public void handlelsRequest() throws IOException{
		System.out.println("Handling ls request");
		if(fileLocationMap2.isEmpty()){
			ObjectOutputStream objectOut = new ObjectOutputStream(DFSSocket.getOutputStream());
			objectOut.writeObject(new ArrayList<String>(fileLocationMap2.keySet()));
			objectOut.close();
		}else{
			ObjectOutputStream objectOut = new ObjectOutputStream(DFSSocket.getOutputStream());
			System.out.println("The file location map keyset is : " + fileLocationMap2.keySet());
			objectOut.writeObject(new ArrayList<String>(fileLocationMap2.keySet()));
			objectOut.close();
		}
	}

	//updateMasterFileList makes changes to the Master File list local to the system when it receives a put command
	public void updateMasterFileList(ArrayList<Integer> nodeKeyList, String fileName){
		
		if(!fileLocationMap.isEmpty() && fileLocationMap.containsKey(fileName)){
			fileLocationMap.get(fileName).addAll(nodeKeyList);
		}
		else if(!fileLocationMap.isEmpty() && !fileLocationMap.containsKey(fileName)
				|| fileLocationMap.isEmpty())
			fileLocationMap.put(fileName, nodeKeyList);
	}
	
	public void updateLocalFileList(String fileName){
		
		if(!localFileListMap.isEmpty() && localFileListMap.containsKey(fileName)){
			localFileListMap.put(fileName, System.currentTimeMillis());
		}
		else if(!localFileListMap.isEmpty() && !localFileListMap.containsKey(fileName)
				|| localFileListMap.isEmpty())
			localFileListMap.put(fileName, System.currentTimeMillis());
	}
}

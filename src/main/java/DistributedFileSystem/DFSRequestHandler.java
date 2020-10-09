package DistributedFileSystem;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;

/**
 * @author Manik, Neha
 *
 * DFSRequestHandle class will run on each node, 
 * listening to other nodes - source nodes. This will also handle the master 
 * and how it will react when contacted by a source node. 
 * It is multi-threaded and accepts multiple client requests
 */

public class DFSRequestHandler implements Runnable{

	ArrayList<String> masterList = new ArrayList<String>();
	private File localFileName;
	static Map<Integer, ChordNode> copyOfLocalMembTbl = new HashMap<Integer, ChordNode>();
	private ServerSocket DFSServerSocket = null;
	private Socket DFSSocket = null;
	private DataInputStream input = null;
	private DataOutputStream output = null;
	private int generalDFSPort = Constants.masterTCPPort;
	//public static DFSTableEntry dfsTable ;
	public static HashMap<String, DFSTableEntry> fileLocationMap2 = new HashMap<String, DFSTableEntry>();
	static HashMap<String, ArrayList<Integer>> fileLocationMap = new HashMap<String, ArrayList<Integer>>(); 
	public static HashMap<String, Long> localFileListMap = new HashMap<String, Long>();
	public static boolean running = true;
	public static boolean replicateNodes = false;
	
	synchronized public static  HashMap<String, ArrayList<Integer>> getFileTable(){
		
		return fileLocationMap;
	}
	
	public DFSRequestHandler() throws IOException{
			copyOfLocalMembTbl = ChordNodeAction.local_MembershipTable;
			DFSServerSocket = new ServerSocket(generalDFSPort);
			 
	}
	
	@Override
	public void run() {
		
		
		
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSRequestHandler.this.getClass().getSimpleName() + "] : " + "Starting the DFS Request Handler Main Class");
		try {
			createFolders("DFSFileSystem");
			createFolders("LocalFiles");
			receiveNodeRequests();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	public void receiveNodeRequests() throws IOException{

		String userInput = null;
		int requestNodeKey = 0;
		String requestNodeAdd = null;
		//If need be, make this as 1 for Node and keep at 3 for Master
		ExecutorService executor = Executors.newFixedThreadPool(3);
		Long startTime = System.currentTimeMillis();
		Long endTime ;
		
		while(running) {
			//Checking whether replication is required or not
			if(!replicateNodes){
				endTime = System.currentTimeMillis();
				if(endTime-startTime>600000){
					startTime = System.currentTimeMillis();
					DFSMasterUpdate dfsRep = new DFSMasterUpdate(replicateNodes, fileLocationMap2, copyOfLocalMembTbl);
					Thread dfsNodeRep = new Thread(dfsRep);
					dfsNodeRep.start();
				}else{/*Moving on*/}
			}else{
				replicateNodes = false;
				DFSMasterUpdate dfsRep = new DFSMasterUpdate(replicateNodes, fileLocationMap2, copyOfLocalMembTbl);
				Thread dfsNodeRep = new Thread(dfsRep);
				dfsNodeRep.start();
			}
			try{
				DFSServerSocket.setSoTimeout(0);
				DFSSocket = DFSServerSocket.accept();
				System.out.println("["+ System.currentTimeMillis() + ":" +DFSRequestHandler.this.getClass().getSimpleName() + "] : " + "Waiting for new DFS Request");
				//DFSSocket = DFSServerSocket.accept();
				copyOfLocalMembTbl = ChordNodeAction.local_MembershipTable;
				System.out.println("Connection received");
				//Check for the first msg coming in
				//Send a confirm if the file is not under process else 
				
				System.out.println("=======================");
				System.out.println("+                     +");
				System.out.println("+Starting user request+");
				System.out.println("+                     +");
				System.out.println("=======================");
				
				executor.execute(new DFSRequestThread(DFSSocket,copyOfLocalMembTbl, fileLocationMap, fileLocationMap2 ));
				
				DFSSocket = null;
				
				
				
				System.out.println("Waiting for another connection");
			}catch (SocketTimeoutException e){
			
			}
			
			
		}
	}
	
	synchronized public static void updateMasterFileTable(ArrayList<Integer> nodeKeyList, String fileName){
		if(!fileLocationMap.isEmpty() && fileLocationMap.containsKey(fileName)){
			fileLocationMap.get(fileName).addAll(nodeKeyList);
		}
		else if(!fileLocationMap.isEmpty() && !fileLocationMap.containsKey(fileName)
				|| fileLocationMap.isEmpty())
		{fileLocationMap.put(fileName, nodeKeyList);}
	}
	
	synchronized public static void updateMasterFileTable(HashMap<String, ArrayList<Integer>> temp_FileMap){
		//Fetch the main table and merge it with this
		ArrayList<Integer> hk_list = new ArrayList<Integer>();
		for(Entry<String, ArrayList<Integer>> pair : temp_FileMap.entrySet()){
			System.out.println("Checking entry for ... " + pair.getKey());
			if(fileLocationMap.containsKey(pair.getKey()) && fileLocationMap.get(pair.getKey()).contains(pair.getValue())!=true){
				hk_list.clear();
				hk_list.addAll(fileLocationMap.get(pair.getKey()));
				hk_list.addAll(pair.getValue());
				fileLocationMap.put(pair.getKey(), hk_list);
				System.out.println("Stored entry for ... " + pair.getKey() + " with list " + hk_list);
				
			}else{
				fileLocationMap.put(pair.getKey(), pair.getValue());	
			}
		}
		
		System.out.println("The master file location has been updated");
		
		
	}
	
	
	public void createFolders(String folderName) throws IOException, InterruptedException{
        System.out.println("Creating new folder.." + folderName);
		String folderRemove = "rm -r " + folderName;
        String folderCreate = "mkdir " + folderName;
        String[] command = {"/bin/sh","-c",folderRemove};
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(command);
        process.waitFor();
        String[] command1 = {"/bin/sh","-c",folderCreate};
        Runtime runtime1 = Runtime.getRuntime();
        Process process1 = runtime.exec(command1);
        process1.waitFor();
        
	}

}
		


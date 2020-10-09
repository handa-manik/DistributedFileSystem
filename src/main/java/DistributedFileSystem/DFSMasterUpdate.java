package DistributedFileSystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Manik, Neha
 *
 * DFSMaterUpdate class performs the following functions:
 * 1. Allows node to send its local file table to the new Master
 * 2. Used by Master to initiate file replication
 * Contains helper function for Socket Connection and input/output stream setup
 */
public class DFSMasterUpdate implements Runnable {

	HashMap<String, ArrayList<Integer>> fileLocationTable = new HashMap<String, ArrayList<Integer>>();
	String ipAdr = null;
	Socket masterSocket = null;
	DataOutputStream masterOut = null;
	DataInputStream masterInput = null;
	int port = Constants.masterTCPPort;
	static HashMap<String, ArrayList<Integer>> fileLocationMap = new HashMap<String, ArrayList<Integer>>(); 
	static HashMap<String, DFSTableEntry> fileLocationMap2 = new HashMap<String, DFSTableEntry>(); 
	static Map<Integer, ChordNode> copyOfLocalMembTbl = new HashMap<Integer, ChordNode>();
	private Socket DFSSocket = null;
	static HashMap<String, DFSTableEntry> replicationTable = new HashMap<String, DFSTableEntry>(); 
	
	static boolean replicateNodes = false;
	
	
	public  DFSMasterUpdate(String ipAdr){
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSMasterUpdate.this.getClass().getSimpleName() + "] : " + "Launching the thread to send data to the master");
		this.ipAdr = ipAdr;
		fileLocationMap = ChordNodeAction.dfs.getFileTable();
	}
	
	public DFSMasterUpdate(boolean replicateNodes, HashMap<String, DFSTableEntry> fileLocationMap2, Map<Integer, ChordNode> copyOfLocalMembTbl) throws UnknownHostException{
		this.replicateNodes = replicateNodes;
		//this.ipAdr = InetAddress.getLocalHost().getHostAddress();
		this.fileLocationMap2 = fileLocationMap2;
		this.copyOfLocalMembTbl = copyOfLocalMembTbl;
		
	}
	
	@Override
	public void run() {
		
		if(replicateNodes){
			//save the order of neighbors in an array
			
			//Review the file table and store in an array a new type of class - filename, source hk, destination list
			getRepFiles();
			
			//the files being replicated should be moved to 'write'
			//create multiple threads which: connects to source, gives it replicate nodes command and then sends the file and nodes in a map
			ExecutorService executor = Executors.newFixedThreadPool(3);
			for(Entry<String, DFSTableEntry> pair : replicationTable.entrySet()){
				String fileName = pair.getKey();
				System.out.println("Starting replication for file : " + fileName);
				String ipAdr = copyOfLocalMembTbl.get(pair.getValue().getSource()).ipAddress;
				ArrayList<Integer> destList = pair.getValue().getDestList();
				executor.execute(new DFSReplicate(fileName, ipAdr, destList));
			}
			//on the source end, it starts a DFSPut and has the destination node list, and maintains a connection to the master, so once everything is done, it sends an ok
			//once ok received, the file is moved back to ""
			while(replicationTable.size()!=0){
				ArrayList<String> remove = new ArrayList<String>();
				for(Entry<String, DFSTableEntry> pair : replicationTable.entrySet()){
					if(pair.getValue().getAction().equalsIgnoreCase("")){
						remove.add(pair.getKey());
						System.out.println("File has been replicated : " + pair.getKey());
					}
				}
				for(int i=0;i<remove.size();i++){
					fileLocationMap2.get(remove.get(i)).setAction("");
				}
			}
		}else{
			fetchFileTable();
			try {
				connectToMaster();
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSMasterUpdate.this.getClass().getSimpleName() + "] : " + "Ending the Master Update Thread");
	}

	private void getRepFiles() {
		
		//Iterate over the File Location Map and checking which file is less than the replication factor
		int repFac = Constants.Rep_Factor;
		
		for(Entry<String, DFSTableEntry> pair : fileLocationMap2.entrySet()){
			int nodeNum = pair.getValue().hashKeyList.size();
			
			if(nodeNum<repFac){
				DFSTableEntry dfst = new DFSTableEntry();
				
				if(pair.getValue().getAction().equalsIgnoreCase("Writing")){
					
					dfst.setFileName(pair.getKey());
					System.out.println("The filename set is : " + dfst.getFileName());
					//replicationTable.put(dfst.getFileName(), dfst);
					System.out.println("The file : " + dfst.getFileName() + " is being updated, will replicate in next round");
					break;
				}
				int source = pair.getValue().getDestList().get(0);
				int dest =0;
				pair.getValue().setAction("Writing");
				
				dfst.setFileName(pair.getKey());
				System.out.println("The filename set is : " + dfst.getFileName());
				dfst.setSource(source);
				System.out.println("The source hashkey is : " + source);
				dest = source;
				while(nodeNum<repFac){
					System.out.println("The missing nodes are : " + (repFac-nodeNum));
					dest = copyOfLocalMembTbl.get(dest).predecessor.hashKey;
					dfst.setDestList(dest);
					System.out.println("The newly added destination is : " + dest);
					nodeNum = pair.getValue().hashKeyList.size();
					System.out.println("The updated node num is : " + nodeNum);
				}
				System.out.println("Adding file : -----" + dfst.getFileName() + "------ to the replication table"  );
				dfst.setAction("Writing");
				replicationTable.put(dfst.getFileName(), dfst);
			}
		}
	}

	public void fetchFileTable(){
		fileLocationTable = ChordNodeAction.dfs.getFileTable();
	}
	
	//create TCP connection with the Master Node
	public void connectToMaster() throws UnknownHostException, IOException, ClassNotFoundException{
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSMasterUpdate.this.getClass().getSimpleName() + "] : " + "Connecting to Master");
		
		
			try {
				masterSocket = connectionSetup(ipAdr);
			}catch(IOException e) {
				System.out.println(e);	
			}
			masterOut = new DataOutputStream(masterSocket.getOutputStream());
			masterInput = new DataInputStream(masterSocket.getInputStream());
			
			
			
			if(!replicateNodes){
				System.out.println("Sending the updateMasterTable code...");
				masterOut.writeUTF("updateMastertable");
				
				ObjectOutputStream objectOutput = new ObjectOutputStream(masterOut);
				
				objectOutput.writeObject(fileLocationMap);
				System.out.println("Transmission of master file complete..");
				objectOutput.close();
			}else{
				System.out.println("Sending replicate nodes command");
				masterOut.writeUTF("ReplicateNodes");
			}
			connectionClose();	
		}
			
	public Socket connectionSetup(String ipAddress) throws UnknownHostException, IOException{
	Socket tempSocket;
	tempSocket = new Socket(InetAddress.getByName(ipAddress), port);

	System.out.println("["+ System.currentTimeMillis() + ":" +DFSMasterUpdate.this.getClass().getSimpleName() + "] : " + "Connected");
	
	return tempSocket;
}
	
	public void connectionClose() throws IOException{
		System.out.println("["+ System.currentTimeMillis() + ":" +DFSMasterUpdate.this.getClass().getSimpleName() + "] : " + "Closing Connection...");
		masterInput.close();
		masterOut.close();
		masterSocket.close();
	}
}

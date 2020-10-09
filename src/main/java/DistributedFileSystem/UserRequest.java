package DistributedFileSystem;
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This thread is for handling SDFS commands entered by the user.  
 */

/**
 * This function parses the user command and handles it appropriately. 
 */

public class UserRequest implements Runnable{
	
	static ArrayList<String> masterList = new ArrayList<String>();
	private File localFileName;
	public static Map<Integer, ChordNode> copyOfLocalMembTbl = new HashMap<Integer, ChordNode>();
	int hashKey;
	ChordNode node = null;
	
	//get the list of Masters from the Membership table and initiate the Server for message handling of UserRequest
	public UserRequest(ChordNode node) throws IOException, InterruptedException {
		this.node = node;
		if(ChordNodeAction.local_MembershipTable.isEmpty() && ChordNodeMain.x){
			ChordNodeAction.local_MembershipTable = ChordNodeMain.Membership_Table;}
		copyOfLocalMembTbl = ChordNodeAction.local_MembershipTable;
		//get the list of Masters from the MembershipTable
		for(Integer ch: copyOfLocalMembTbl.keySet()){
			
			if(copyOfLocalMembTbl.get(ch).isMaster){
				System.out.println("The master is at : " + copyOfLocalMembTbl.get(ch).ipAddress);
				masterList.add(copyOfLocalMembTbl.get(ch).ipAddress);
			}
		}
		
		System.out.println("The Master list is : " + masterList);
	}//end of Client Constructor
	@Override
	public void run(){
		hashKey = node.hashKey;
		String userInput = ""; //string to read message from the input
		BufferedReader br
          = new BufferedReader(new InputStreamReader(System.in));
		
		//read User command to interact with distributed file system
		while(true){
			System.out.println("Enter a command for file processing::");
			try {
				
				userInput = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {

				
				long start = System.currentTimeMillis();
				processUserRequest(userInput);
				long end = System.currentTimeMillis();
				long timetaken = end - start;

				
			} catch (InterruptedException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void processUserRequest(String userInput) throws InterruptedException, IOException
	{
		masterList.clear();
		hashKey = node.hashKey;
		Map<Integer, ChordNode> temp_Membership_Table = new HashMap<Integer, ChordNode>();
		String []command = userInput.split(" ");
		
		if(command[0].equalsIgnoreCase("Put") || command[0].equalsIgnoreCase("Get")){
			if(command.length!=3){
				System.out.println("Invalid format");
				return;}
		}else if(command[0].equalsIgnoreCase("remove") || command[0].equalsIgnoreCase("locate")){
			if(command.length!=2){
				System.out.println("Invalid format");
				return;
			}
		}else{}
		
		if(ChordNodeMain.x){
			System.out.println("Yes, gateway");
			temp_Membership_Table = ChordNodeMain.Membership_Table;
			System.out.println("["+ System.currentTimeMillis() + ":"+UserRequest.class.getSimpleName() + "] : " +"The Membership List is : " +
					temp_Membership_Table.keySet()	);
		}else{
			System.out.println("["+ System.currentTimeMillis() + ":"+UserRequest.class.getSimpleName() + "] : " +"The Membership List is : " +
					ChordNodeAction.local_MembershipTable.keySet());
			temp_Membership_Table = ChordNodeAction.local_MembershipTable;
		}
		
		for(Integer ch: temp_Membership_Table.keySet()){
			
			if(temp_Membership_Table.get(ch).isMaster){
				System.out.println("The master is at : " + temp_Membership_Table.get(ch).ipAddress);
				masterList.add(temp_Membership_Table.get(ch).ipAddress);
			}
		}
		
		System.out.println("The Master list is : " + masterList);
		
		
		switch (command[0].toLowerCase()){
			case "put":
				System.out.println("Putting file " + command[1] + " as file name : " + command[2]);
				DFSPut fPut = new DFSPut(command[1], command[2], masterList, temp_Membership_Table);
				Thread putThread = new Thread(fPut);
				putThread.start();
				break;
				
			case "get":
				System.out.println("Waiting for file from the server::");
				DFSGet fGet = new DFSGet(command[2],command[1], masterList, temp_Membership_Table);
				Thread getThread = new Thread(fGet);
				getThread.start();
				break;
				
			case "ls":
				System.out.println("Waiting for list of files from the server::");
				DFSls fls = new DFSls(masterList, temp_Membership_Table);
				Thread lsThread = new Thread(fls);
				lsThread.start();
				break;
				
			case "remove":
				System.out.println("File to remove request sent to the server::");
				DFSRemove fRemove = new DFSRemove(command[1], masterList, temp_Membership_Table);
				Thread rmThread = new Thread(fRemove);
				rmThread.start();
				break;
				
			case "locate":
				System.out.println("Waiting for list of machines that contain the copy of the requested file::");
				DFSLocate fLocate = new DFSLocate(command[1], masterList, temp_Membership_Table);
				Thread locThread = new Thread(fLocate);
				locThread.start();
				break;
				
			case "lshere":
				System.out.println("List of all files stored on the local machine");
				if(!DFSRequestHandler.fileLocationMap2.keySet().isEmpty()){
					System.out.println("Files");
					for(Entry<String, DFSTableEntry> pair : DFSRequestHandler.fileLocationMap2.entrySet()){
						System.out.println(pair.getKey() + " : " + pair.getValue().getHashKeyList());
					}
					
				}else{
					System.out.println("No file found");
				}
				break;
				
			case "masterlist":
				System.out.println("Finding the master.....");
				//masterList.clear();
				for(Integer ch: temp_Membership_Table.keySet()){
					
					if(temp_Membership_Table.get(ch).isMaster){
						System.out.println("The master is at : " + temp_Membership_Table.get(ch).ipAddress);
					}
				}
				System.out.println("The master list is : " + masterList);
				break;
				
			case "gateway":
				System.out.println("Finding the Gateway.....");
				masterList.clear();
				for(Integer ch: temp_Membership_Table.keySet()){
					
					if(temp_Membership_Table.get(ch).isGateway()){
						System.out.println("The gateway is at : " + temp_Membership_Table.get(ch).ipAddress);
						masterList.add(temp_Membership_Table.get(ch).ipAddress);
					}
				}
				System.out.println("The gateway is is : " + masterList);
				break;
				
			case "hashkey":
				System.out.println("The local hashkey is : " + node.hashKey);
				break;
				
			case "ml":

				String ml = "";
				
				for(int i=0; i<temp_Membership_Table.size()+1; i++){
					System.out.println("The hashkey is " + hashKey);
					ml = ml + String.valueOf(hashKey) + "-->";
					hashKey = temp_Membership_Table.get(hashKey).successor.hashKey;
					System.out.println("The updated Hashkey is " + hashKey);
				}
				System.out.println(ml);
				break;
			
			case "leave":

					System.out.println("Sending msg to all nodes");
					FailureDetector.node_ip = node.ipAddress;
					FailureDetector.FD_ipAdr.clear();
					
					for(int i=0;i<temp_Membership_Table.size();i++){
						FailureDetector.FD_ipAdr.add(temp_Membership_Table.get(hashKey).ipAddress);
						hashKey = temp_Membership_Table.get(hashKey).successor.hashKey;
					}
					FailureDetector.FD_ipAdr.remove(node.ipAddress);
					ChordNodeMain.chordNodeAction.running = false;
					FailureDetector.updateNodes();
					System.out.println("!!!!All nodes informed!!!!!");
					System.out.println("Closing all threads!");
					while(true){
						System.out.println("Checking if threads are still running");
						if(ChordNodeMain.chordActionThread.getState().equals(Thread.State.TERMINATED)){
							System.out.println("@@@@@@@@ The ChordNode Action Thread has terminated @@@@@@@@@");
							break;
						}
						Thread.sleep(5000L);
					}
					System.out.println("@@@@@@@ All Thread Closed @@@@@@@@");

				
				break;
				
				
			case "replicatenodes":
				//activate only for master
				
				if(masterList.contains(node.ipAddress)){
					System.out.println("Activating Replicate Node Flag...");
					ChordNodeAction.dfs.replicateNodes = true;
				}else{
					System.out.println("Not Valid for Node");
				}
				break;
				

		}
	}	
}

package DistributedFileSystem;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

/**
 * @author Manik, Neha
 *
 * GatewayThread class performs the following function
 * 1. Receives remote node information from the newly joined node to add to the group
 * 2. Updates membership table to be sent to new nodes joining the group 
 * 3. Sends the Membership table to successor and predecessor nodes of the newly joined node
 * 	 Action 1 and 2 performed over TCP Connection and Action 3 over UDP Connection
 */

public class GatewayThread implements Runnable,Serializable {
	
	InetAddress ipAddress;
	int port;
	Map<Integer, ChordNode> RemoteNode_MemTable;
	public static boolean running = true;
	
	//A TCP connection thread waits for the new node to come and then sends the ML to the new Node
	public GatewayThread(InetAddress ipAddress, int port){
		this.ipAddress = ipAddress;
		this.port = port;
	}
	
	/* Makes tcp connection with the newly joined node to exchange the updated membership table
	 * Throws error if connection fails, or binding cannot be performed, or information received is 
	 * in correct format 
	 */
	@Override
	public void run() {
		ChordNode RemoteNode = null;
		System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Gateway thread started");
		System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Join Request processing started.....");
		ServerSocket tcp = null;
		
		//opens ServerSocket port for binding
        try {
        	System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Opening sever port at " + port);
            tcp = new ServerSocket(port);
            System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Port opened");
            System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Waiting for Client connection");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        while (running){
            Socket joinRequest = null;
            
            //accepts the new Remote Server join Request to add to the membership table
            try {
            	 tcp.setSoTimeout(20000);
            	joinRequest=tcp.accept();
                System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Connection received with " + joinRequest.getPort());
            
            
            //reads the Node data from the Remote Server
            try {
				ObjectInputStream objectInput = new ObjectInputStream(joinRequest.getInputStream());
				RemoteNode = (ChordNode) objectInput.readObject();
				
			} catch (IOException | ClassNotFoundException e2) {
				e2.printStackTrace();
			}
            
            //updates the main membership table
            updateMembershipTable(RemoteNode); 
            try{
            	updateRemoteNode(joinRequest);
            }catch(IOException e){
            	e.printStackTrace();
            }
 
            //close socket connection
            try {
                joinRequest.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            } catch(IOException e) {
           	 
           }
        }
        System.out.println("Closing Gateway Thread...");
	}
	/* This function performs the following actions:
	 * 1. Updates data of the Gateway Node Membership table upon the joining of the new Remote Server
	 * 	  Gateway node maintains the full membership list
	 * 2. Membership for newly joined remote node created with the logic of having 3 predecessors 
	 * 	  and two successors in case total number of nodes in the Chord ring are more than 5. 
	 * 	  This is done to make the system scalable and fault tolerant up to 3 nodes.
	 */
	
	public void updateMembershipTable(ChordNode RemoteNode){
		
		if(!ChordNodeMain.Membership_Table.containsKey(RemoteNode.getHashedId()))
			ChordNodeMain.Membership_Table.put(RemoteNode.getHashedId(), RemoteNode);
		
		List ChordNodeList = new ArrayList(ChordNodeMain.Membership_Table.keySet());
		//sort the collection of Chord Nodes to generate the predecessor and the successor of each node
		Collections.sort(ChordNodeList);
		int currentHashKey;
		
		if(ChordNodeList.size() >1 ){
			for(int i = 0; i< ChordNodeList.size(); i++){
				if( i == 0 ){
					currentHashKey = (int)ChordNodeList.get(ChordNodeList.size() - 1);
					ChordNodeMain.Membership_Table.get(ChordNodeList.get(i)).predecessor = ChordNodeMain.Membership_Table.get(currentHashKey);
					currentHashKey =  (int)ChordNodeList.get(1);
					ChordNodeMain.Membership_Table.get(ChordNodeList.get(i)).successor = ChordNodeMain.Membership_Table.get(currentHashKey);
				}
				else if( i == ChordNodeList.size() - 1 ){
					currentHashKey =  (int)ChordNodeList.get(0);
					ChordNodeMain.Membership_Table.get(ChordNodeList.get(i)).successor = ChordNodeMain.Membership_Table.get(currentHashKey);
					currentHashKey = (int)ChordNodeList.get(i-1);
					ChordNodeMain.Membership_Table.get(ChordNodeList.get(i)).predecessor = ChordNodeMain.Membership_Table.get(currentHashKey);
				}
				else{
					currentHashKey = (int)ChordNodeList.get(i - 1);
					ChordNodeMain.Membership_Table.get(ChordNodeList.get(i)).predecessor = ChordNodeMain.Membership_Table.get(currentHashKey);
					currentHashKey =  (int)ChordNodeList.get(i + 1);
					ChordNodeMain.Membership_Table.get(ChordNodeList.get(i)).successor = ChordNodeMain.Membership_Table.get(currentHashKey);
				}
			}
		}
		if(ChordNodeList.size() > 1 ){
		RemoteNode_MemTable = new HashMap<Integer, ChordNode>();
		RemoteNode_MemTable = ChordNodeMain.Membership_Table;
			for(Integer key : ChordNodeMain.Membership_Table.keySet()){
				System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Printing values for " + key );
				ChordNode temp = ChordNodeMain.Membership_Table.get(key).predecessor ;
				System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "The pred hashkey is : " + temp.hashKey);
				temp = ChordNodeMain.Membership_Table.get(key).successor;
				System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "The suc hashkey is : " + temp.hashKey);
			}
			System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Printing all values of the table: ");
		}
	}
	
	//Remote Nodes(two successors and three predecessors) updated with the newly joined node data
	public void updateRemoteNode(Socket joinRequest) throws IOException{
		
		System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Sending Membership_Table to Remote Nodes in the Chord");
		
		ObjectOutputStream objectOutput = new ObjectOutputStream(joinRequest.getOutputStream());
		objectOutput.writeObject(RemoteNode_MemTable);
		objectOutput.flush();
		objectOutput.close();
		
		//this sets flag for heartbeater and listener to rebuild its membership table
		ChordNodeMain.setUpdateHBTable(true);
		ChordNodeMain.setUpdateListenerTable(true);
		
		System.out.println("["+GatewayThread.this.getClass().getSimpleName() + "]  : " + "Membership_Table Transmission Complete");
	}
	


}

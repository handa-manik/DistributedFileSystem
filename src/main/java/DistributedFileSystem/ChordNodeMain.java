package DistributedFileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * @author Manik, Neha
 *
 * This is the entry point where the main function decides
 * whether the Server instance is a gateway node or node and
 * then makes call to appropriate class functions (GatewayThread or ChordNodeAction)
 *
 */

public class ChordNodeMain {
	public static ChordNode node;
	String ipAddress;
	int port;
	static Map<Integer, ChordNode> Membership_Table;
	public static ChordNodeAction chordNodeAction = null;
	static boolean x = false;
	public static GatewayThread gateThread;
	public static Thread chordActionThread;
	static boolean updateHBTable = false; //flag will be set to true if new node joins
	synchronized public static boolean isUpdateHBTable() { //ensures when update action is performed, get function cannot be called
		return updateHBTable;
	}

	synchronized public static void setUpdateHBTable(boolean updateHBTable) { //ensures when update action is performed, get function cannot be called
		ChordNodeMain.updateHBTable = updateHBTable;
	}
	static boolean updateListenerTable = false; //flag set to true when new node joins in
	synchronized public static boolean isUpdateListenerTable() {
		return updateListenerTable;
	}

	synchronized public static void setUpdateListenerTable(boolean updateListenerTable) {
		ChordNodeMain.updateListenerTable = updateListenerTable;
	}
	/* 
	 * 1.GatewayThread called if the Server instance is the gateway node matching ip address
	 * 2. ChordNodeAction called for any new joining node whose Ip Address does not match the Gateway 
	 * IP address
	 */
	
	public ChordNodeMain(String ipAddress, int port){
		node = new ChordNode(ipAddress, port);
	}
	
	public static void main(String args[]) throws IOException, InterruptedException{
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeMain.class.getSimpleName() + "] : " +"Starting program..........");
		InetAddress ipAddress = InetAddress.getLocalHost();
		
		String ipAddress_str = ipAddress.getHostAddress();
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeMain.class.getSimpleName() + "] : " +
		"IP Address of the current Server Node::" +ipAddress.getHostAddress().toString());
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeMain.class.getSimpleName() + "] : " +
		"Checking if the current Server IPAddress is the designated Gateway Node Address");
		new ChordNodeMain(ipAddress_str, Constants.ChordNode_TCPPort);
		
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeMain.class.getSimpleName() + "] : " +"Welcome to the Network");
		
		if(ifGatewayNode(ipAddress_str)){
			x=true;
			node.setGateway(true);
			node.setMaster(true);
			createMembershipTable();
			runGatewayThread(ipAddress,Constants.GatewayTCPPort);
			chordNodeAction = new ChordNodeAction();
			chordActionThread = new Thread(chordNodeAction);
			chordActionThread.start();
			//Closing as it is started in ChordNodeAction also
			/*Thread dfsReqHlr = new Thread(new DFSRequestHandler());
			dfsReqHlr.start();*/
			
		}else if(!args[0].isEmpty() && args[0].equalsIgnoreCase("Master")){
			node.setMaster(true);	
			chordNodeAction = new ChordNodeAction( Constants.GatewayNodeAddress,Constants.GatewayTCPPort );
			chordActionThread = new Thread(chordNodeAction);
			chordActionThread.start();
		}else{
			chordNodeAction = new ChordNodeAction( Constants.GatewayNodeAddress,Constants.GatewayTCPPort );
			chordActionThread = new Thread(chordNodeAction);
			chordActionThread.start();
		}
		//Starting the user command center
		Thread usrReqThread = new Thread(new UserRequest(node));
		usrReqThread.start();
	}
	
	/* ifGatewayNode returns true if new node ipaddress matches the designated gateway node 
	 * IP address, else returns false
	 */
	private static boolean ifGatewayNode(String ipAddress){
		if(Constants.GatewayNodeAddress.equals(ipAddress)){
			System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeMain.class.getSimpleName() + "] : " +"The instance is a gateway node");
			return true;
		}
		else
			return false;
	}
	
	/* initiate the gateway thread if the incoming node ip address matches the designated
	 * IP address of the gateway node
	 */
	private static void runGatewayThread(InetAddress ipAddress, int port){
		gateThread = new GatewayThread(ipAddress, port );
		Thread gateway = new Thread(gateThread);
		gateway.start();
	}
	
	//Called once when the gateway node is initiated for the first time 
	private static void createMembershipTable(){
		Membership_Table = new HashMap<Integer, ChordNode>();
		if(node.getHashedId() != 0){
			System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeMain.class.getSimpleName() + "] : " +"Adding data to the Finger Table");
			Membership_Table.put(node.getHashedId(), node);
		}
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeMain.class.getSimpleName() + "] : " + 
		"Current Element in the Membership Table::" + Membership_Table.get(node.getHashedId()).hashKey);
	}
}

package DistributedFileSystem;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Manik, Neha
 *
 * Listener class performs the following function
 * 1. Listens to heartbeat periodically from the predecessor nodes
 * 2. Maintains a heartbeat table for the pings received from the predecessor nodes
 * 3. Locks the heartbeat table when getting information to avoid using any stale data 
 */

public class Listener implements Runnable {

	public static Map<String, HeartBeatRecord> heartBeat_Table = new HashMap<String, HeartBeatRecord> ();
	
	synchronized public static Map<String, HeartBeatRecord> getHeartBeat_Table() {
		return heartBeat_Table;
	}

	public void setHeartBeat_Table(Map<String, HeartBeatRecord> heartBeat_Table) {
		this.heartBeat_Table = heartBeat_Table;
	}

	HashSet<String> all_ip = new HashSet<String>();
	int local_HashKey=0 ;
	int heartBeatTimer = 500;
	int localPort = Constants.HB_Rec_UDPPort;
	
	int HeartBeat_Counter = 0;
	long HeartBeat_TimeStamp = 0;
	long Last_HeartBeat_TimeStamp = 0;
	public static boolean running = true;
	public static FailureDetector failureDetect = null;
	public static Thread failureDetector = null;
	public Listener(){
		local_HashKey = ChordNodeMain.node.hashKey;
		
	}
	
	/* run() calls the failure detector and hear Hearbeat functions to perform appropriate actions.
	 * Error thrown if the thread input output stream format is incorrect
	 */
	@Override
	public void run() {
		construct_heartBeat_Table();
		try {
			
			//Starting the failureDetecto thread which observes the HeartBeatTable for anyone not pinging in last x secs
			failureDetect = new FailureDetector();
			//failureDetector.setName("failureDetector");
			failureDetector = new Thread(failureDetect);
			failureDetector.start();
			hear_HeartBeat();
			System.out.println("@@@@@@@@@ Closing Listener Thread @@@@@@@@@@@");
			failureDetect.running = false;
			
			System.out.println("Listener Closed....");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//Listens to heartbeat periodically from the predecessor nodes
	public void hear_HeartBeat() throws IOException{
		System.out.println("["+ System.currentTimeMillis() + ":" +Listener.this.getClass().getSimpleName() + "] : " + "Hearing heartbeat....");
		DatagramPacket dataPacket;
		InetAddress remoteIp ;
		DatagramSocket hear_hb = new DatagramSocket(localPort);
		hear_hb.setReceiveBufferSize(2048);
		byte[] msgBytes = new byte[2048];
		int limit = msgBytes.length;
		long latest_HeartBeat = 0;
		int heartBeat_HashKey = 0;
		
		while(running){
			if(ChordNodeMain.updateListenerTable){construct_heartBeat_Table();
				ChordNodeMain.updateListenerTable = false;
			}
			
			dataPacket = new DatagramPacket(msgBytes, limit);
			hear_hb.setSoTimeout(5000);
			try{
				hear_hb.receive(dataPacket);
				if(heartBeat_Table.isEmpty() || ChordNodeMain.updateListenerTable == true ){continue;}
				String s = null ;

				latest_HeartBeat = System.currentTimeMillis();
				//Only if the msg is coming from a valid hb sender, the update of HB table should run, else discard the msg
				if(all_ip.contains(dataPacket.getAddress().getHostAddress().toString())){
					System.out.println("["+ System.currentTimeMillis() + ":" + Listener.this.getClass().getSimpleName() + "] : " 
							+ "DataPacket received from : " + dataPacket.getSocketAddress() + " at HashKey " + 
							heartBeat_Table.get(dataPacket.getAddress().getHostAddress()).getHashKey());
					update_heartBeat_Table(dataPacket.getAddress().getHostAddress().toString(),latest_HeartBeat);
				}
			}catch (SocketTimeoutException e){
				
			}
			
				
		}	//end of while loop
	} //end of hear_heartbeat function
	
	/*Calling get function locks the HeartBeat Table and prevents any 
	 * other thread from using stale data
	 */
	synchronized public void update_heartBeat_Table(String ipAddress, long timeStamp){
		System.out.println("["+ System.currentTimeMillis() + ":" +Listener.this.getClass().getSimpleName() + "] : " + "Updating Heartbeat Table for : " + ipAddress + " at : " + timeStamp);
		
		getHeartBeat_Table().get(ipAddress).setTimeStamp(timeStamp);
	}
	
	/*This function creates a listener table which will be used for listening to the predecessor pings 
	 * and maintaining the heartbeat table*/
	public void construct_heartBeat_Table(){
		
		all_ip.clear();
		System.out.println("["+ System.currentTimeMillis() + ":" +Listener.this.getClass().getSimpleName() + "] : " + 
		"Fetching the local HashKey and the Total Membership list");
		int currentHashKey=local_HashKey;
		String ipAdr;
		heartBeat_Table.clear();
		if(ChordNodeMain.x)
			ChordNodeAction.local_MembershipTable = ChordNodeMain.Membership_Table;
		
		for(int i=0;i<2;i++){
			System.out.println("["+ System.currentTimeMillis() + ":" +Listener.this.getClass().getSimpleName() + "] : " + 
					"The local Hashkey is : " + local_HashKey);
			ChordNode temp = ChordNodeAction.local_MembershipTable.get(currentHashKey).predecessor;
			System.out.println("["+ System.currentTimeMillis() + ":" +Listener.this.getClass().getSimpleName() + "] : " + 
			"The pred hashkey of : " + currentHashKey + " is "+ temp.hashKey);
			
			HeartBeatRecord hbrec = new HeartBeatRecord();
			
			currentHashKey = temp.hashKey;
			ipAdr = ChordNodeAction.local_MembershipTable.get(currentHashKey).ipAddress;
			System.out.println("["+ System.currentTimeMillis() + ":" +Listener.this.getClass().getSimpleName() + "] : " + "The pred ip is : " + ipAdr);
			hbrec.setHashKey(currentHashKey);
			if( currentHashKey == local_HashKey )
				continue;
			all_ip.add(ipAdr);
			heartBeat_Table.put(ipAdr, hbrec);
		}//end of for loop
		System.out.println("["+ System.currentTimeMillis() + ":" +Listener.this.getClass().getSimpleName() + "] : " + 
		"Listening to the following nodes : " + heartBeat_Table.keySet());
	}	//end of construct_hearbeat_table function
}

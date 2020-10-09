package DistributedFileSystem;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.Map.Entry;

/**
 * @author Manik, Neha
 *
 * HeartBeater class performs the following function
 * 1. Heartbeat table constructed locally for each node
 * 2. Local node sends heartbeat periodically to Remote successor nodes via UDP
 */

public class HeartBeater implements Runnable{
	public static Map<Integer, String> heartBeat_Table = new HashMap<Integer, String> ();
	int local_HashKey =0;
	int heartBeatTimer = 500;
	int localPort = Constants.HB_Send_UDPPort;
	int remotePort = Constants.HB_Rec_UDPPort;
	int HeartBeat_Counter = 0;
	long HeartBeat_TimeStamp = 0;
	long Last_HeartBeat_TimeStamp = 0;
	public static boolean running = true;
	
	public HeartBeater(){
		local_HashKey = ChordNodeMain.node.hashKey;
	}
	
	/* calls the Construct and Send Hearbeat functions to perform appropriate actions.
	 * Error thrown if the thread is interrupted by other process or input output stream format 
	 * is incorrect
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("["+ System.currentTimeMillis() + ":" +HeartBeater.this.getClass().getSimpleName() + "] : " +
		"Constructing a table of Nodes to which heartbeat will be sent");
		construct_HeartBeat_Table();
		try {
			send_HeartBeat();
			System.out.println("HeartBeater Closed...");
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//function to construct the hearbeat table locally for each node with the successor node info
	public void construct_HeartBeat_Table(){
		System.out.println("["+ System.currentTimeMillis() + ":" +HeartBeater.this.getClass().getSimpleName() + "] : " +
				"Fetching the local HashKey and the Total Membership list");
		if(ChordNodeMain.x)
			ChordNodeAction.local_MembershipTable = ChordNodeMain.Membership_Table;
		int currentHashKey=local_HashKey;
		heartBeat_Table.clear();
		for(int i=0;i<2;i++){
			currentHashKey = ChordNodeAction.local_MembershipTable.get(currentHashKey).successor.hashKey;
			heartBeat_Table.put(currentHashKey, ChordNodeAction.local_MembershipTable.get(currentHashKey).ipAddress);
		}
		heartBeat_Table.remove(local_HashKey);
		System.out.println("["+ System.currentTimeMillis() + ":" +HeartBeater.this.getClass().getSimpleName() + "] : " +
		"The Heartbeat will be sent to : " + heartBeat_Table.keySet());
	}
	
	//Sends heartbeat to successors periodically via UDP
	public void send_HeartBeat() throws IOException, InterruptedException{
		System.out.println("["+ System.currentTimeMillis() + ":" +HeartBeater.this.getClass().getSimpleName() + "] : " + "Sending heartbeat....");
		String msg = null;
		byte[] msgBytes = new byte[10];
		
		int limit =0;
		int ci=0;
		DatagramPacket dataPacket;
		InetAddress remoteIp ;
		DatagramSocket send_hb = new DatagramSocket(localPort);
		
		while(running){
			if(ChordNodeMain.isUpdateHBTable()){
				System.out.println("[" + System.currentTimeMillis() + ":" +HeartBeater.this.getClass().getSimpleName() + "] : " + 
			"New node in the Membership Table, updating Heartbeat Table...");
				construct_HeartBeat_Table();
				ChordNodeMain.setUpdateHBTable(false);
			}
			HeartBeat_TimeStamp = System.currentTimeMillis();
		
			if(((HeartBeat_TimeStamp - Last_HeartBeat_TimeStamp)>Constants.HB_Timer) && heartBeat_Table.size()>0){
				HeartBeat_Counter++;
				for(Entry<Integer, String> pair : heartBeat_Table.entrySet()){
					msg = "Alive";
					msgBytes = msg.getBytes();
					limit = msg.getBytes().length;
					System.out.println("["+ System.currentTimeMillis() + ":" +HeartBeater.this.getClass().getSimpleName() + "] : " + "Created message : " +
					msg + " and message is : " + new String(msgBytes));
					remoteIp = InetAddress.getByName(pair.getValue());
					dataPacket = new DatagramPacket(msgBytes, limit , remoteIp ,remotePort);
					send_hb.send(dataPacket);
					System.out.println("["+ System.currentTimeMillis() + ":" +HeartBeater.this.getClass().getSimpleName() + "] : " +
					"Last Heartbeat sent to : " + dataPacket.getAddress() + " with HashKey " + pair.getKey());
				}
				
				Last_HeartBeat_TimeStamp = System.currentTimeMillis();
				System.out.println("["+ System.currentTimeMillis() + ":" +HeartBeater.this.getClass().getSimpleName() + "] : " + 
				"Last Heartbeat Counter : " + HeartBeat_Counter);
				System.out.println("["+ System.currentTimeMillis() + ":" +HeartBeater.this.getClass().getSimpleName() + "] : " +
				"Last TimeStamp : " + HeartBeat_TimeStamp);
			}
		}	//end of while loop
	}//end of send_Heartbeat function
}

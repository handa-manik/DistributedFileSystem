package DistributedFileSystem;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Manik, Neha
 * FailureDetector class performs the following actions:
 * 1. Check is made in the Listener table for a node if an update is not received against a specific timestamp
 * 2. Updates Remote successor and Remote predecessor nodes of the failed node regarding its failure
 */

public class FailureDetector implements Runnable {

	Map<String, HeartBeatRecord> FD_HeartBeat_Table;
	//Map<ipAddress, HeartBeat(timestamp;hashkey)>
	static Map<Integer, ChordNode> FD_Membership_Table = new HashMap<Integer, ChordNode>();
	Listener faultListener = new Listener();
	public static int local_hashkey = 0;
	String local_ipAddr = null;
	public static Set<String> FD_ipAdr = new HashSet<String>();
	static String node_ip = null;
	public static boolean running = true;
	public static boolean beMaster = false;
	public static boolean beGateway = false;
	public static byte[] ack_msg = new byte[2048];
	public static DatagramPacket msgReceived = new DatagramPacket(ack_msg, ack_msg.length);
	
	
	public FailureDetector() throws UnknownHostException{
		System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " 
				+ "Starting Failure Detection..." );
		this.local_hashkey = ChordNodeMain.node.hashKey;
		this.local_ipAddr = InetAddress.getLocalHost().getHostAddress();
		
		if(ChordNodeMain.x){
			FD_Membership_Table = ChordNodeMain.Membership_Table;
		}else{
			FD_Membership_Table = ChordNodeAction.local_MembershipTable;
		}
	}
	
	//thread started with call to reconNodes and update nodes function
	@Override
	public void run(){
		
		while(running){
			try {
				reconNodes();
				System.out.println("Failure Detector Closed....");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	/* Checks in the listener table against a set timestamp to find if an update has 
	 * not been received from a predecessor node for a while
	 * If a failure is detected the successor of the failed node is updated
	 */
	public void reconNodes() throws IOException{
		
		long currentTS = 0;
		long node_Last_TS = 0;
		int node_hk = 0;
		ChordNode tempNode;
		int temp_hashKey = 0;
		int start_hashKey=0;
		
		while(running){
			if(ChordNodeMain.x){
				FD_Membership_Table = ChordNodeMain.Membership_Table;
			}else{
				FD_Membership_Table = ChordNodeAction.local_MembershipTable;
			}
			try{
			FD_HeartBeat_Table =  new HashMap<String, HeartBeatRecord>(Listener.getHeartBeat_Table());
			}catch (ConcurrentModificationException e){
				break;
			}
			if(FD_HeartBeat_Table.size()<1){
				continue;
			}
			currentTS = System.currentTimeMillis();
			for(Entry<String, HeartBeatRecord> pair : FD_HeartBeat_Table.entrySet()){
				if(String.valueOf(pair.getValue().getTimeStamp()) != null && pair.getValue().getTimeStamp() != 0){
					
					node_Last_TS = pair.getValue().getTimeStamp();
					
					if((currentTS-node_Last_TS)>Constants.HB_FaultTime){
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + "!!!!!!!!!!!!Time difference::" +((currentTS-node_Last_TS)/1000));
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
								"!!!!!!!!!!!!!!!!"  );
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
								"!!!!!!!!!!!!!!!!"  );
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
								"!!!!!!!!!!!!!!!!"  );						
						
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + "Current time : " + (currentTS/1000) + " and Latest Time : " + (node_Last_TS/1000));
						node_hk = pair.getValue().getHashKey();
						node_ip = pair.getKey();
						
												
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
					"!!!!!!!!!!!!!!!!Failure detected of node with ip address : " + node_ip + " and Hash key : " + node_hk );
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
								"!!!!!!!!!!!!!!!!"  );
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
								"!!!!!!!!!!!!!!!!"  );
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
								"!!!!!!!!!!!!!!!!"  );
						
						//Calculating the position of the failed node and storing the ipAddress of the nodes to be informed
						temp_hashKey = local_hashkey;
						for(int i =0;i<2;i++){
							tempNode = FD_Membership_Table.get(temp_hashKey).predecessor;
							if(tempNode.ipAddress.equalsIgnoreCase(node_ip)){
								if(i==0){
									if(tempNode.isGateway()){beMaster=true;}
									else if(tempNode.isMaster()){beGateway=true;}
									else if (tempNode.isMaster()==true && tempNode.isGateway()==true){beMaster=true;beGateway=true;}
									tempNode = FD_Membership_Table.get(local_hashkey).successor;
									start_hashKey = tempNode.getHashedId() ;
								}else{
									start_hashKey = local_hashkey;
								}
							 break;
							}else{
								temp_hashKey = tempNode.hashKey;
								//tempNode = FD_Membership_Table.get(temp_hashKey).predecessor;
							}
						} //end of inner for loop
						
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
								"Collating list of Nodes to be informed about the failure " + node_ip );
						tempNode = FD_Membership_Table.get(start_hashKey);
						
						for(int i =0;i<5;i++){
							temp_hashKey = tempNode.hashKey;
							FD_ipAdr.add(tempNode.ipAddress);
							tempNode = FD_Membership_Table.get(temp_hashKey).predecessor;
						}//end of inner for loop
						FD_ipAdr.add(Constants.GatewayNodeAddress);
						FD_ipAdr.remove(node_ip);
						FD_ipAdr.remove(InetAddress.getLocalHost().getHostAddress());
						
						System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
								"Node Failure message will be sent to the following IPs : " + FD_ipAdr);
						
						rebuildLocalMembershipTable(node_hk);
						ChordNodeMain.updateHBTable=true;
						ChordNodeMain.updateListenerTable = true;
						
						try {
							updateNodes();
							updateNodes2();
						} catch (IOException e) {
							e.printStackTrace();
						}
						
					} //end of outer if - condition
				}
				}//end of outer for loop
			
			if((beMaster==true || beGateway==true) ){
				System.out.println("A node left and this node became the Master or Gateway, informing others");
				updateNodes2();
			}
				
				
		} //end of while loop
	} //end of recon Nodes function
	
	/* Updates Remote successor and Remote predecessor nodes of the failed node regarding its failure */
	public static void updateNodes() throws IOException {
		System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.class.getSimpleName() + "] : " + 
	"Informing relevant nodes of the Failed node");
		
		byte[] msg = new byte[2048];
		byte[] ack_msg = new byte[2048];

		DatagramPacket send_UN ;
		DatagramPacket rec_Ack;
		DatagramSocket updateNode_send = ChordNodeAction.MT_send;
		DatagramSocket updateNode_rec = ChordNodeAction.MT_receive;
		
		
		node_ip = node_ip + "Failed";
		
		msg = node_ip.getBytes();
		
		boolean ack = false;
		while(!ack){
			for(String s:FD_ipAdr){
				InetAddress ip = InetAddress.getByName(s);
				send_UN = new DatagramPacket(msg,msg.length,ip,Constants.MT_Rec_UDPPort);
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.class.getSimpleName() + "] : " + 
				"Sending the node fail to : " + ip + " at port : " + Constants.MT_Rec_UDPPort);
				updateNode_send.send(send_UN);
			} //end of inner for loop
			
			//Waiting for ack from receivers
			long start = System.currentTimeMillis();
			long end =0;
			while((end-start)<5000){
				try{
					rec_Ack = new DatagramPacket(ack_msg,ack_msg.length);
					updateNode_rec.setSoTimeout(5000);
					updateNode_rec.receive(rec_Ack);
					
						if(rec_Ack.getData()!=null && FD_ipAdr.contains(rec_Ack.getAddress().getHostAddress())){
							System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.class.getSimpleName() + "] : " + "Ack received from : " + rec_Ack.getAddress().getHostAddress());
							FD_ipAdr.remove(rec_Ack.getAddress().getHostAddress());
						}
					
					end = System.currentTimeMillis();
					
				}catch(IOException e){
					//NA
					end = System.currentTimeMillis();
				}
				if(FD_ipAdr.size()==0){
					ack=true;
					System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.class.getSimpleName() + "] : " + 
					"All nodes have received the msg, ending the updates now");
				}
			} //end of inner while loop			
		}//end of while loop
		//updateNode_send.close();
		//updateNode_rec.close();
	} //end of updateNodes function
	
	public void rebuildLocalMembershipTable(int hashkey) {
		
		if(ChordNodeMain.x){
			if(ChordNodeMain.Membership_Table.containsKey(hashkey)){
				ChordNode failedNode = ChordNodeMain.Membership_Table.get(hashkey);
				ChordNode failedNodePredecessor = failedNode.predecessor;
				ChordNode failedNodeSuccessor = failedNode.successor;
				
				failedNodePredecessor.successor = failedNodeSuccessor;
				failedNodeSuccessor.predecessor = failedNodePredecessor;
				ChordNodeMain.Membership_Table.remove(hashkey);
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + "The updated Membership list is : " + ChordNodeMain.Membership_Table.keySet());
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
			}
		}
		else
		{
			if(ChordNodeAction.local_MembershipTable.containsKey(hashkey)){
				ChordNode failedNode = ChordNodeAction.local_MembershipTable.get(hashkey);
				ChordNode failedNodePredecessor = failedNode.predecessor;
				ChordNode failedNodeSuccessor = failedNode.successor;
				
				failedNodePredecessor.successor = failedNodeSuccessor;
				failedNodeSuccessor.predecessor = failedNodePredecessor;
				ChordNodeAction.local_MembershipTable.remove(hashkey);
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + "The updated Membership list is : " + ChordNodeAction.local_MembershipTable.keySet());
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
			}
		}
		/*Assigning the new master right now, but will soon have this by bully algo*/
		if(beMaster==true && beGateway!=true){
			ChordNodeMain.node.setMaster(true);
			
		}else if(beMaster!=true && beGateway==true){
			ChordNodeMain.node.setGateway(true);
			
		}else if(beMaster==true && beGateway==true){
			ChordNodeMain.node.setGateway(true);
			ChordNodeMain.node.setMaster(true);
			
		}
		
	}//end of updateAfterNodeLeave function
	public static void updateNodes2() throws IOException {
		System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.class.getSimpleName() + "] : " + 
	"Informing relevant nodes of the New Gateway/Master");
		
		byte[] msg = new byte[2048];
		byte[] ack_msg = new byte[2048];

		DatagramPacket send_UN ;
		DatagramPacket rec_Ack;
		DatagramSocket updateNode_send = ChordNodeAction.MT_send;
		DatagramSocket updateNode_rec = ChordNodeAction.MT_receive;
		
		if(beMaster==true && beGateway==false){
			beMaster=false;
			node_ip = "Master";
		}else if (beGateway==true && beMaster == false){
			beGateway = false;
			node_ip = "Gateway";
		}else if(beMaster==true && beGateway==true){
			beMaster=beGateway=false;
			node_ip="GatewayMaster";
		}else{}
		
		if(FD_ipAdr.isEmpty()){
			for(Entry<Integer, ChordNode> pair: FD_Membership_Table.entrySet() ){
				FD_ipAdr.add(pair.getValue().ipAddress);
			}
			FD_ipAdr.remove(FD_Membership_Table.get(local_hashkey).ipAddress);
			System.out.println("The new info : " + node_ip + " will be sent to : " + FD_ipAdr);
			if(FD_ipAdr.isEmpty()){
				System.out.println("No nodes to be informed about the info " + node_ip);
				return;
			}
		}
		
		
		
		msg = node_ip.getBytes();
		
		boolean ack = false;
		while(!ack){
			for(String s:FD_ipAdr){
				InetAddress ip = InetAddress.getByName(s);
				send_UN = new DatagramPacket(msg,msg.length,ip,Constants.MT_Rec_UDPPort);
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.class.getSimpleName() + "] : " + 
				"Sending the node message to : " + ip + " at port : " + Constants.MT_Rec_UDPPort);
				updateNode_send.send(send_UN);
			} //end of inner for loop
			
			//Waiting for ack from receivers
			long start = System.currentTimeMillis();
			long end =0;
			while((end-start)<5000 && msgReceived!=null){
				/*rec_Ack = new DatagramPacket(ack_msg,ack_msg.length);
				updateNode_rec.setSoTimeout(5000);
				updateNode_rec.receive(rec_Ack);*/
				System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.class.getSimpleName() + "] : " +"The msgReceived is : " + new String(msgReceived.getData(),0,msgReceived.getLength()));
				
				if(msgReceived.getData()!=null && FD_ipAdr.contains(msgReceived.getAddress().getHostAddress())){
					System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.class.getSimpleName() + "] : " + "Ack received from : " + msgReceived.getAddress().getHostAddress());
					FD_ipAdr.remove(msgReceived.getAddress().getHostAddress());
				}
				end = System.currentTimeMillis();
				msgReceived = null;
				if(FD_ipAdr.size()==0){
					ack=true;
					System.out.println("["+ System.currentTimeMillis() + ":" +FailureDetector.class.getSimpleName() + "] : " + 
					"All nodes have received the msg, ending the updates now");
				}
			} //end of inner while loop			
		}//end of while loop
		//updateNode_send.close();
		//updateNode_rec.close();
	} //end of updateNodes function
	
} //end of FailureDetector class

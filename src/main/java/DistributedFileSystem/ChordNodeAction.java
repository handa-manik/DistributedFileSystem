package DistributedFileSystem;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Map.Entry;

/**
 * @author Manik, Neha
 *
 * ChordNodeACtion class performs the following function
 * 1. Receives membership table data from the Gateway Node
 * 2. Sends updated node data to the immediate two successors 
 * 3. Sends the Membership table to successor and predecessor nodes of the newly joined node
 * 4. Updates Heartbeater and Listener when a new node is added or an existing node fails
 * 	 Action 1 and 2 performed over TCP Connection
 * 	 Action 3 and 4 over UDP Connection
 */

public class ChordNodeAction implements Runnable{

	Socket chordSocket;
	int port;
	String gatewayAddress;
	Map<Integer, ChordNode> local_FingerTable = new HashMap<Integer, ChordNode> ();
	static Map<Integer, ChordNode> local_MembershipTable = new HashMap<Integer, ChordNode> ();
	int local_HashKey = ChordNodeMain.node.hashKey;
	public static DatagramSocket MT_receive = null;
	public static DatagramSocket MT_send = null;
	public static boolean running = true;
	boolean beMaster = false;
	boolean beGateway = false;
	public static DFSRequestHandler dfs = null;
	public static Thread dfsReqHlr = null;
	
	
	public ChordNodeAction(String gatewayAddress,int port) throws SocketException{
	
		this.port = port;
		this.gatewayAddress = gatewayAddress;
		MT_receive = new DatagramSocket(Constants.MT_Rec_UDPPort);
		MT_receive.setReceiveBufferSize(2048);
		MT_send = new DatagramSocket(Constants.MT_Send_UDPPort);
		MT_send.setSendBufferSize(2048);
	}
	
	public ChordNodeAction() throws SocketException{
		MT_receive = new DatagramSocket(Constants.MT_Rec_UDPPort);
		MT_receive.setReceiveBufferSize(2048);
		MT_send = new DatagramSocket(Constants.MT_Send_UDPPort);
		MT_send.setSendBufferSize(2048);

	}

	@Override
	public void run() {
		
		try {
			if(!ChordNodeMain.x){
			Join();
			 
				if(local_MembershipTable.size()>2){
					updateNodes();
				}
			}
			dfs = new DFSRequestHandler();
			 dfsReqHlr = new Thread(dfs);
			dfsReqHlr.start();			
			HeartBeater hb = new HeartBeater();
			Thread heartBeater = new Thread(hb);
			heartBeater.start();
			Listener ln = new Listener();
			Thread Listener = new Thread(ln);
			Listener.start();			
			receiveUpdates();
			System.out.println("ChordNodeAction Closing down....");
			System.out.println("Closing Heartbeater...");
			hb.running = false;
			System.out.println("Closing Listener...");
			ln.running = false;
			System.out.println("Closing DFS request Handler...");
			dfs.running = false;
			System.out.println("Closing Gateway Thread....");
			ChordNodeMain.gateThread.running = false;
			
			while(true){
				Thread.sleep(5000L);
				System.out.println("Heartbeater : " + heartBeater.getState());
				System.out.println("Listener : " + Listener.getState());
				if(heartBeater.getState().equals(Thread.State.TERMINATED) && Listener.getState().equals(Thread.State.TERMINATED)){
					System.out.println("@@@@@@@@@@@ The Heartbeater and Listener have been closed @@@@@@@@@");
					break;
				}
			}
			System.out.println("@@@@@@@@@ Closing ChordNode Action Thread @@@@@@@@");
			
			
			} catch (IOException | ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/* Makes TCP connection with the gateway node to send the node information and 
	 * receive the updated membership table 
	 * Throws error if connection fails, or binding cannot be performed, or information received is 
	 * in correct format 
	 */
	public void Join() throws IOException, ClassNotFoundException{
		//Creates a TCP connection with the Gateway node
		System.out.println( "["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + "Connecting to the Gateway at : " + gatewayAddress + " at port : " + port);
		try {
			chordSocket = new Socket(gatewayAddress , port);
			System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + "Connection done");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + "Receiving node info");
		ObjectOutputStream objectOutput = new ObjectOutputStream(chordSocket.getOutputStream());
        objectOutput.writeObject(ChordNodeMain.node);
        
        ObjectInputStream objectInput = new ObjectInputStream(chordSocket.getInputStream());
        local_MembershipTable = (Map<Integer, ChordNode>) objectInput.readObject();
        System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + "Data in local finger table::" +local_MembershipTable.keySet()); 
	}
	
	//update successors and predecessors with the new node information and updated successor and predecessor
	public void updateNodes() throws IOException {
		System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
	"Informing relevant nodes of the new node");
		Map<Integer, ChordNode> partial_MembershipTable = new HashMap<Integer, ChordNode> ();
		Set<Integer> temp_keySet = local_MembershipTable.keySet();
		System.out.println(temp_keySet);
		ChordNode temp_local = null;
		int temp_HashKey = 0;
		Set<String> temp_ipAdr = new HashSet<String>();
		byte[] msg;
		byte[] ack_msg = new byte[1];

		DatagramPacket send_UN ;
		DatagramPacket rec_Ack;
		temp_HashKey = local_MembershipTable.get(local_HashKey).successor.getHashedId();
		temp_HashKey = local_MembershipTable.get(temp_HashKey).successor.getHashedId();
		
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + "The local HashKey of is : " + local_HashKey);
		
			for(int i=0;i<5;i++){
				temp_local = local_MembershipTable.get(temp_HashKey);
				partial_MembershipTable.put(temp_HashKey, temp_local);
				//temp_ipAdr.add(temp_local.ipAddress);
				System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + "Added the node with HashKey " + temp_local.getHashedId() );
				temp_HashKey = temp_local.predecessor.hashKey;
				
			}
			
			for(Integer k : local_MembershipTable.keySet()){
				temp_ipAdr.add(local_MembershipTable.get(k).ipAddress);
				System.out.println("The hashkey is : " + k + " and the ip address is : " + local_MembershipTable.get(k).ipAddress);
			}	
			
		temp_ipAdr.remove(InetAddress.getLocalHost().getHostAddress());
		temp_ipAdr.remove(Constants.GatewayNodeAddress);
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + "The following nodes will be sent over the UDP : " + partial_MembershipTable.keySet());
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + "Writing the partial membership table to bytes");
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream(2048);
		ObjectOutputStream op = new ObjectOutputStream(bos);
		
		op.writeObject(partial_MembershipTable);
		op.close();
		msg = bos.toByteArray();
		bos.close();
		
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + "The size of packet is " + msg.length);
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
				"Sending the required nodes using port : " + Constants.MT_Send_UDPPort 
				+ " to the following IPs : " + temp_ipAdr );
		boolean ack = false;
		while(!ack){
			for(String s:temp_ipAdr){
				InetAddress ip = InetAddress.getByName(s);
				send_UN = new DatagramPacket(msg,msg.length,ip,Constants.MT_Rec_UDPPort);
				System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
				"Sending the node update to : " + ip + " at port : " + Constants.MT_Rec_UDPPort);
				MT_send.send(send_UN);
			}//end of for loop
			
			//Waiting for ack from receivers
			long start = System.currentTimeMillis();
			long end =0;
			while((end-start)<10000){
				try{
					rec_Ack = new DatagramPacket(ack_msg,ack_msg.length);
					MT_receive.setSoTimeout(10000);
					//updateNode_rec.setSoTimeout(5000);
					MT_receive.receive(rec_Ack);
					if(rec_Ack.getData()!=null && temp_ipAdr.contains(rec_Ack.getAddress().getHostAddress())){
						System.out.println("Ack received from : " + rec_Ack.getAddress().getHostAddress());
						temp_ipAdr.remove(rec_Ack.getAddress().getHostAddress());
					}
					end = System.currentTimeMillis();
				}catch(IOException e){
					end = System.currentTimeMillis();
				}
				if(temp_ipAdr.size()==0){
					ack=true;
					System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
					"All nodes have received the msg, ending the updates now");
				}
			}	//end of inner while loop	
		}//end of outer while loop
	}//end of update nodes function
	
	/* For any newly added and failed node following actions are performed
	 * 1. Updates the local membership table. 
	 * 2. Sends update to Heatbeater and Listener table for the update action to be performed
	 * 3. Sends acknowledgement back to the sender node both for newly added and failure node messages
     */
	public void receiveUpdates() throws ClassNotFoundException, InterruptedException, IOException{
		System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
	"Opening port and receiving any updates sent by any node...");
		
		byte[] rec_UN = new byte[2048];
		byte[] send_UN = new byte[2048];
		String ack = "received";
		send_UN = ack.getBytes();

		Map<Integer, ChordNode> temp_MembershipTable = new HashMap<Integer, ChordNode> ();
		DatagramPacket receive ;
		DatagramPacket sendAckMsg;
		boolean sendAck = true;
		
		while(running){
			
			receive = new DatagramPacket(rec_UN,rec_UN.length);
			System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
			"Waiting for control message...");
			try {
				MT_receive.setSoTimeout(0);
				
				MT_receive.receive(receive);
				String s = new String(receive.getData(),0,receive.getLength());
				
				if(s.equalsIgnoreCase("received")){
					System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + "The messsage received is : " + s);
					System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
							"Rogue Acknowledgement message received, discarding it" );
					FailureDetector.msgReceived = receive;
					continue;
				}
				else if(s.contains("Failed")){
					System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + "The messsage received is : " + s);
					System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
							"!!!!!!!!!!!!!!!!"  );
					System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
							"!!!!!!!!!!!!!!!!"  );
					System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
							"!!!!!!!!!!!!!!!!"  );
					
					s = s.replace("Failed", "");
					System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
							"Node Failure message received from " + receive.getAddress() + " for machine "
							+ s);
					int c =0;
					sendAck = true;
					ack = "received";
					send_UN = ack.getBytes();
					while(sendAck){
						sendAckMsg = new DatagramPacket(send_UN,send_UN.length,receive.getAddress(),Constants.MT_Rec_UDPPort);
						MT_send.send(sendAckMsg);
						System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
						"Acknowledgement message sent to " + receive.getAddress() + " at port"
						+ Constants.MT_Rec_UDPPort);
						c++;
						Thread.sleep(500);
							if(c==2){sendAck = false;}
					}
					if(FailureDetector.node_ip!=null && FailureDetector.node_ip.equalsIgnoreCase(s)){
						System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
								"Current node has already detected this Failed node : " + s);
						continue;
					}
					
					String ipAdr_toRemove = s;
					
					Map<Integer, String> temp_heartBeat_Table = HeartBeater.heartBeat_Table;
					Map<String, HeartBeatRecord> temp_Listener_Table = Listener.getHeartBeat_Table();
					int hashKey_toRemove = 0;
					if(temp_heartBeat_Table.containsValue(ipAdr_toRemove)){
						System.out.println("The node found in Heartbeat table..");
						for(Entry<Integer, String> pair : temp_heartBeat_Table.entrySet()){
							System.out.println("The node under check is : "+ pair.getKey());
							System.out.println("The check of " + pair.getValue() + " against to remove " + ipAdr_toRemove);
							
							if(pair.getValue().equalsIgnoreCase(ipAdr_toRemove)){
								hashKey_toRemove = pair.getKey();
								System.out.println("The hashkey to remove is " + hashKey_toRemove);
								System.out.println("The hashkey of pred is " + ChordNodeMain.node.predecessor.hashKey);
								
								if(local_MembershipTable.get(local_HashKey).predecessor.hashKey == hashKey_toRemove){
									System.out.println("The failing node is a pred...");
									if(local_MembershipTable.get(hashKey_toRemove).isMaster()==true &&
											local_MembershipTable.get(hashKey_toRemove).isGateway()==true)
									{
										System.out.println("Becoming Gateway and Master...");
										beMaster=true;
										beGateway=true;
										updateMemList("GatewayMaster", ChordNodeMain.node.ipAddress);
										Thread gateway = new Thread(new GatewayThread(InetAddress.getLocalHost(), Constants.GatewayTCPPort ));
										gateway.start();
									}
									else if(local_MembershipTable.get(hashKey_toRemove).isGateway()==true){
										System.out.println("Becoming gateway...");
										beGateway=true;
										updateMemList("Gateway", ChordNodeMain.node.ipAddress);
										Thread gateway = new Thread(new GatewayThread(InetAddress.getLocalHost(), Constants.GatewayTCPPort ));
										gateway.start();
									}
									else if (local_MembershipTable.get(hashKey_toRemove).isMaster()==true ){
										System.out.println("Becoming Master...");
										beMaster=true;
										updateMemList("Master", ChordNodeMain.node.ipAddress);
									}
								}
								updateAfterNodeLeave(hashKey_toRemove);
								ChordNodeMain.updateHBTable=true;
								ChordNodeMain.updateListenerTable = true;
								if(beMaster==true && beGateway == true){
									System.out.println("Sending information to other nodes of new roles...Gateway and Master");
									FailureDetector.beGateway =true;
									FailureDetector.beMaster = true;
								}else if (beMaster == true && beGateway != true){
									System.out.println("Sending information to other nodes of new roles...Master");
									FailureDetector.beMaster = true;
								}else if (beMaster != true && beGateway == true){
									System.out.println("Sending information to other nodes of new roles...Gateway");
									FailureDetector.beGateway = true;
								}
							}//end of if
						}//end of for
					}else if(temp_Listener_Table.containsKey(ipAdr_toRemove)){
						System.out.println("The node found in Listener table");
						for(Entry<String, HeartBeatRecord> pair : temp_Listener_Table.entrySet()){
							if(pair.getKey().equalsIgnoreCase(ipAdr_toRemove)){
								hashKey_toRemove = pair.getValue().getHashKey();
								if(local_MembershipTable.get(local_HashKey).predecessor.hashKey == hashKey_toRemove){
									if(local_MembershipTable.get(hashKey_toRemove).isMaster()==true &&
											local_MembershipTable.get(hashKey_toRemove).isGateway()==true)
									{
										beMaster=true;
										beGateway=true;
										updateMemList("GatewayMaster", ChordNodeMain.node.ipAddress);
										Thread gateway = new Thread(new GatewayThread(InetAddress.getLocalHost(), Constants.GatewayTCPPort ));
										gateway.start();
									}
									else if(local_MembershipTable.get(hashKey_toRemove).isGateway()==true){
										beGateway=true;
										updateMemList("Gateway", ChordNodeMain.node.ipAddress);
										Thread gateway = new Thread(new GatewayThread(InetAddress.getLocalHost(), Constants.GatewayTCPPort ));
										gateway.start();
									}
									else if (local_MembershipTable.get(hashKey_toRemove).isMaster()==true ){
										beMaster=true;
										updateMemList("Master", ChordNodeMain.node.ipAddress);
										
									}
									
									
								}
								
								updateAfterNodeLeave(hashKey_toRemove);
								ChordNodeMain.updateHBTable=true;
								ChordNodeMain.updateListenerTable = true;
								if(beMaster==true && beGateway == true){
									FailureDetector.beGateway =true;
									FailureDetector.beMaster = true;
								}else if (beMaster == true && beGateway != true){
									FailureDetector.beMaster = true;
								}else if (beMaster != true && beGateway == true){
									FailureDetector.beGateway = true;
								}
							}
						}
					}
					else{
						System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
								"Acknowledgement message sent to " + receive.getAddress() + " at port"
								+ Constants.MT_Rec_UDPPort);
						System.out.println("Removing the failed/left node from the ML");
						Map<Integer, ChordNode> temp_ML = new HashMap<Integer, ChordNode> ();
						temp_ML = local_MembershipTable;
						int keyHash = temp_ML.get(local_HashKey).predecessor.hashKey;
						System.out.println("Finding the node in the mem table...");
						for(int i=0;i<temp_ML.size();i++){
							String temp_ip = temp_ML.get(keyHash).ipAddress;
								if(temp_ip.equalsIgnoreCase(ipAdr_toRemove)){
									hashKey_toRemove = keyHash;
									if(i==0){
						//Compare this part against above one to see if this is right			
									if(temp_ML.get(keyHash).isGateway()==true&& temp_ML.get(keyHash).isMaster()==true )
									{beMaster=true;
									beGateway=true;}
									else if(temp_ML.get(keyHash).isMaster()){beMaster=true;}
									else if (temp_ML.get(keyHash).isGateway()==true){beMaster=true;}
									}
									updateAfterNodeLeave(hashKey_toRemove);
									ChordNodeMain.updateHBTable=true;
									ChordNodeMain.updateListenerTable = true;
									break;
								}
							keyHash = temp_ML.get(keyHash).predecessor.hashKey;
						}
					}
				}else if(s.equalsIgnoreCase("Master")){
					System.out.println("New Master Found at : " + receive.getAddress().getHostAddress());
					sendAck(receive.getAddress());
					updateMemList(s,receive.getAddress().getHostAddress());
				}else if(s.equalsIgnoreCase("Gateway")){
					System.out.println("New Gateway Found at : " + receive.getAddress().getHostAddress());
					sendAck(receive.getAddress());
					updateMemList(s,receive.getAddress().getHostAddress());
				}else if(s.equalsIgnoreCase("GatewayMaster")){
					System.out.println("New GatewayMaster Found at : " + receive.getAddress().getHostAddress());
					sendAck(receive.getAddress());
					updateMemList(s,receive.getAddress().getHostAddress());	
				}
				else
				{
					System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + "Packet size is " + receive.getData().length);
					ObjectInputStream iStream = new ObjectInputStream(new ByteArrayInputStream(receive.getData()));
					temp_MembershipTable = (Map<Integer, ChordNode>) iStream.readObject();
					iStream.close();
					
					System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
					"The size of the updated table is : " + temp_MembershipTable.size());
						if(temp_MembershipTable.size()>0){
							if(ChordNodeMain.x){
								ChordNodeMain.Membership_Table.putAll(temp_MembershipTable);
							}else{
								local_MembershipTable.putAll(temp_MembershipTable);
							}
							
							ChordNodeMain.updateHBTable=true;
							ChordNodeMain.updateListenerTable = true;
							
							int c=0;
							sendAck = true;
							while(sendAck){
								sendAckMsg = new DatagramPacket(send_UN,send_UN.length,receive.getAddress(),Constants.MT_Rec_UDPPort);
								MT_send.send(sendAckMsg);
								System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
								"Acknowledgement message sent to " + receive.getAddress() + " at port"
								+ Constants.MT_Rec_UDPPort);
								c++;
								Thread.sleep(500);
									if(c==2){sendAck = false;}
							}
						}
					}//end of if-else

				} catch (IOException e) {
					e.printStackTrace();
				}
		}//end of while loop
	}//end of receiveupdates function
	
	/* For any node leaving the ring the following actions will be performed
	 * 1. Updates the local membership table.
	 * 2. Updates the master membership table. 
	 * 3. Updates the predecessor and successor nodes
     */
	public void updateAfterNodeLeave(int hashkey) {
		
		if(ChordNodeMain.x){
			if(ChordNodeMain.Membership_Table.containsKey(hashkey)){
				ChordNode failedNode = ChordNodeMain.Membership_Table.get(hashkey);
				ChordNode failedNodePredecessor = failedNode.predecessor;
				ChordNode failedNodeSuccessor = failedNode.successor;
				
				failedNodePredecessor.successor = failedNodeSuccessor;
				failedNodeSuccessor.predecessor = failedNodePredecessor;
				ChordNodeMain.Membership_Table.remove(hashkey);
				System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + "!!!!!!!!!!!!!!!The updated Membership list is : " + ChordNodeMain.Membership_Table.keySet());
				System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
				System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
				System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
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
				System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + "!!!!!!!!!!!!!The updated Membership list is : " + ChordNodeAction.local_MembershipTable.keySet());
				System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
				System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
				System.out.println("["+ System.currentTimeMillis() + ":" +ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
						"!!!!!!!!!!!!!!!!"  );
			}
		}
	}//end of updateAfterNodeLeave function
	
	DFSMasterUpdate dfsMU ;
	Thread dfsMasterUpdate ;
	
	//updates the membership list local node and sends and update to the Gateway Master node
	public void updateMemList(String msg, String ipAdr) throws UnknownHostException{
		System.out.println("Updating the Membership list for minor updates");
		String newIpAdr = ipAdr;
		String updateMsg = msg;
		
		for(Entry<Integer, ChordNode> pair: local_MembershipTable.entrySet()){
			if(pair.getValue().ipAddress.equalsIgnoreCase(newIpAdr)){
				System.out.println( updateMsg + " found at HashKey : " + pair.getKey());
				if(updateMsg.equalsIgnoreCase("Master")){
					pair.getValue().setMaster(true);
					if(!InetAddress.getLocalHost().getHostAddress().equalsIgnoreCase(newIpAdr)){
					dfsMU = new DFSMasterUpdate(newIpAdr);
					dfsMasterUpdate = new Thread(dfsMU);
					dfsMasterUpdate.start();
					}
				}else if(updateMsg.equalsIgnoreCase("Gateway")){
					pair.getValue().setGateway(true);
				}else if(updateMsg.equalsIgnoreCase("GatewayMaster")){
					pair.getValue().setGateway(true);
					pair.getValue().setMaster(true);
					if(!InetAddress.getLocalHost().getHostAddress().equalsIgnoreCase(newIpAdr)){
						dfsMU = new DFSMasterUpdate(newIpAdr);
						dfsMasterUpdate = new Thread(dfsMU);
						dfsMasterUpdate.start();
						}
				}else{}
			}
		}
		System.out.println(updateMsg + " Node updated");
			
	}
	//Function to send an acknowledge to the sender when an action is succeeded
	public void sendAck(InetAddress rec_ip) throws IOException, InterruptedException{
		System.out.println("Sending acknowledgement...");
		int c =0;
		boolean sendAck = true;
		String ack = "received";
		byte[] send_UN = ack.getBytes();
		while(sendAck){
			DatagramPacket sendAckMsg = new DatagramPacket(send_UN,send_UN.length,rec_ip,Constants.MT_Rec_UDPPort);
			MT_send.send(sendAckMsg);
			System.out.println("["+ System.currentTimeMillis() + ":"+ChordNodeAction.this.getClass().getSimpleName() + "] : " + 
			"Acknowledgement message sent to " + rec_ip + " at port"
			+ Constants.MT_Rec_UDPPort);
			c++;
			Thread.sleep(500);
				if(c==2){sendAck = false;}
		}
	}
	
}//end of class


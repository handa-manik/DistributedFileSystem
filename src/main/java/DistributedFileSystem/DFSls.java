package DistributedFileSystem;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Manik, Neha
 *
 * DFSGet class performs the following functions:
 * 1. Handles end user Get request
 * 2. Creates TCP connection with the Master Node to fetch the Node list
 * 3. Gets the file name from the Nodes as received from the Master by connecting to DFSRequestHandler
 * 4. Writes requested file to the Remote Node
 * Contains helper function for Socket Connection and input/output stream setup
 */

public class DFSls implements Runnable{
	//initialize socket and input stream
	private Socket dfsGetSocket = null;
	private DataInputStream input = null;
	private DataOutputStream output = null;
	ArrayList<String> localMasterList = new ArrayList<String>();
	int port = Constants.masterTCPPort;
	Map<Integer, ChordNode> copyOfLocalMembTbl = new HashMap<Integer, ChordNode>();
	ArrayList<String> lsFileList = new ArrayList<String>();

	public DFSls(ArrayList<String> masterList, Map<Integer, ChordNode> copyOfLocalMembTbl){
		localMasterList = masterList;
		this.copyOfLocalMembTbl = copyOfLocalMembTbl;	
	}
	
	@Override
	public void run(){
		try {
			connectToMaster();
		} catch (IOException |ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		System.out.println("=====================");
		System.out.println("+                   +");
		System.out.println("+Ending user request+");
		System.out.println("+                   +");
		System.out.println("=====================");
		
	}
	
	//create TCP connection with the Master Node
	public void connectToMaster() throws UnknownHostException, IOException, ClassNotFoundException{
		
		for(String ml: localMasterList){
			try {
				System.out.println("DFSLS: Creating connection with : " + ml  );
				connectionSetup(ml);
			}catch(IOException e) {
				System.out.println(e);	
			}
			output.writeUTF("ls");
			System.out.println("Waiting for file names from the master");
			ObjectInputStream objectInput = new ObjectInputStream(input);
			lsFileList = (ArrayList<String>) objectInput.readObject();
			if(!lsFileList.isEmpty()){
				System.out.println("Information received from the Master....");
				System.out.println("Total Number of Files : " + lsFileList.size());
				System.out.println("List of fs file names in the system::");
				for(int i=0;i<lsFileList.size();i++){
					System.out.println("[ "+i+" ]" + " : " + lsFileList.get(i));
				}
				connectionClose();//closing the connection with the Master Node
				return;
			}else
				System.out.println("No files exists in the file system");
			
			connectionClose();
		}
	}
	
	//Helper function for Socket connection setup
	public void connectionSetup(String ipAddress) throws UnknownHostException, IOException{
		
		dfsGetSocket = new Socket(InetAddress.getByName(ipAddress), port);
		System.out.println("Connected");
		
		//take input from the socket 
		input = new DataInputStream(new BufferedInputStream
				(dfsGetSocket.getInputStream()));
		
		//send output to the socket
		output = new DataOutputStream(dfsGetSocket.getOutputStream());	
	}
	
	//Helper function for Socket and input/output stream close
	public void connectionClose() throws IOException{
		input.close();
		output.close();
		dfsGetSocket.close();
	}
}

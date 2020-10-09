package DistributedFileSystem;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Manik, Neha
 *
 * ChordNode class implements Node and performs the following function
 * Sets up the Node with the data as specified in the Node constructor
 * Getter and Setter functions to handle or update any node related data based on the 
 * data members specified in this class
 */

public class ChordNode implements Node, Serializable {
	
	String ipAddress;
	int port;
	ChordNode successor;
	ChordNode predecessor;
	long timestamp;
	int hashKey = 0;
	boolean isMaster = false;
	
	public boolean isMaster() {
		return isMaster;
	}

	public void setMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}

	boolean gateway = false;
	
    public boolean isGateway() {
		return gateway;
	}

	public void setGateway(boolean gateway) {
		this.gateway = gateway;
	}
	
    /* Constructor */
	public ChordNode(String ipAddress, int port){
		this.ipAddress = ipAddress;
		this.port = port;
		this.successor = this;
		this.predecessor = this;
    	HashedIDGenerator hk = new  HashedIDGenerator();
    	hashKey = hk.HashIDGen(ipAddress);
    	System.out.println("Hash Key::"+ hashKey);
    }

	@Override
	public int getHashedId() {
		return hashKey;
	}

	@Override
	public boolean getRecoverStatus() {
		return false;
	}	
}

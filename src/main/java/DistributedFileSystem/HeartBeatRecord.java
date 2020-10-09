package DistributedFileSystem;

/**
 * @author Manik, Neha
 *
 * HeartBeaterRecord class includes the getter and setter functions referred by the 
 * HeartBeater class to access and set hashkey, timestamp for any given node
 * 
 */
public class HeartBeatRecord {
	long timeStamp = 0;
	int hashKey = 0;

	
	public int getHashKey() {
		return hashKey;
	}

	public void setHashKey(int hashKey) {
		this.hashKey = hashKey;
	}
	
	public long getTimeStamp(){
		return timeStamp;
	}
	
	public void setTimeStamp(long timeStamp){
		this.timeStamp = timeStamp;
		
	}
}

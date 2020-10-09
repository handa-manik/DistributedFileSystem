package DistributedFileSystem;

import java.net.InetAddress;
import java.util.ArrayList;

/**
 * @author Manik, Neha
 *
 * DTSTableEntry class helps build the structure for the File location and File current status
 * 
 */

public class DFSTableEntry {

	String fileName = "";
	ArrayList<Integer> hashKeyList = new ArrayList<Integer>(); 
	String action = "";
	InetAddress ip = null;
	int source = 0;
	
	
	ArrayList<Integer> destList = new ArrayList<Integer>();
	
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public int getSource() {
		return source;
	}
	public void setSource(int source) {
		this.source = source;
	}
	public ArrayList<Integer> getDestList() {
		return destList;
	}
	public void setDestList(Integer dest) {
		destList.add(dest);
	}
	
	public void setDestList(ArrayList<Integer> destList) {
		this.destList.clear();
		this.destList.addAll(destList);
		
	}
	
	public ArrayList<Integer> getHashKeyList() {
		return hashKeyList;
	}
	
	public void setHashKeyList(Integer hashKey) {
		this.hashKeyList.add(hashKey);
		
	}

	public void setHashKeyList(ArrayList<Integer> hashKeyList) {
		this.hashKeyList.clear();
		this.hashKeyList.addAll(hashKeyList);
	}
	
	public String getAction() {
		return action;
	}
	public void setAction(String action) {
		this.action = action;
	}
	public InetAddress getIp() {
		return ip;
	}
	public void setIp(InetAddress ip) {
		this.ip = ip;
	}
	
}

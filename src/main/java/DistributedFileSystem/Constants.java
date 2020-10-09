package DistributedFileSystem;
/**
 * @author Manik, Neha
 *
 * Constants class helps maintain fixed public static values that are available to all classes
 * throughout the code and are not changed in the course of action
 * Naming convention of all the below variables helps understand the use and its appropriate action
 * 
 */

public class Constants {
	public static final String ALIVE = "ALIVE";
	public static final String LEAVE = "LEAVE";		
	public static final String FAILED = "FAILED";
	
	public static String GatewayNodeAddress = "172.31.17.65";
	public static final int GatewayTCPPort = 8000;
	public static final int masterTCPPort = 8500; 
	public static final int HB_Send_UDPPort = 7500;
	public static final int HB_Rec_UDPPort = 7550;
	public static final int MT_Send_UDPPort = 7600;
	public static final int MT_Rec_UDPPort = 7650;
	public static final String NodeGroup1 = "230.0.0.1";
	public static final String GatewayGroup = "230.0.0.0";
	public static final int ChordNode_TCPPort = 9005;
	public static final int HB_Timer = 300000;
	public static final int HB_FaultTime = 600000;
	public static final int VC = 1;
	public static String MasterNodeAddress = "";
	public static final int Rep_Factor = 3;

}

package DistributedFileSystem;


/**
 *  @author Manik, Neha
 * Generate a key based on the given name and key-space. Depending on the key-space, this key may or may not be unique.
 * @param name
 * @return
 */
public class HashedIDGenerator {
	public static final int HASH_BIT = 8;
	
	//function to generate the hask key for any given node IP address
	public int HashIDGen(String IPAddress){
		
		int hashKey = 0;
		long temp = 0;
		for (int i = 0; i < IPAddress.length(); i++)  
	    {
	       temp  = 31 * temp  + IPAddress.charAt(i);
	    }
		hashKey = (int) Math.abs(temp% 256);
		return hashKey;
	}
	
	/**
	 * Decide whether the key k is between f and t in the ring.
	 * @param k - Key
	 * @param f - From
	 * @param t - To
	 * @return true if k is in the interval (f, t]
	 */
	public static boolean between(int k, int f, int t) {
		int key = k;
		int from = f;
		int to = t;
		
		if(from > to) {
			return key > from || key <= to;
		}else if(from < to)
			return key > from && key <= to;
		else
			return true;		
	}
}

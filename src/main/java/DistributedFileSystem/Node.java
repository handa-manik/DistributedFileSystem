package DistributedFileSystem;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Manik, Neha
 * This is an interface that would be implemented by the Chord Node 
 * setting up the Chord ring, adding, leaving or querying Chord Nodes
 */
 
public interface Node {
  /**
   * Get the hashed id of the node
   * @return hashed id
   * @throws RemoteException
   */
  int getHashedId();

  /**
   * Get recovery status of the node
   * @return node recoverStatus flag
   * @throws RemoteException
   */
  boolean getRecoverStatus();

  /**
   * Set recovery status of the node
   * @param flag
   * @throws RemoteException
   */
}

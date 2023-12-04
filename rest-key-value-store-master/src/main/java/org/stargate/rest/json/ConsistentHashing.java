/*
 * ConsistentHashing class implements a simple consistent hashing algorithm
 * for distributing keys across a set of servers, maintaining balance even
 * when servers are added or removed.
 */

package org.stargate.rest.json;
import java.io.*;
import java.util.*;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashing {
  private final SortedMap<Integer, String> ring = new TreeMap<>();
  private final int numberOfReplicas;

  /**
   * Constructs a ConsistentHashing instance with the specified number of replicas.
   * @param numberOfReplicas The number of replicas for each server in the ring.
   */
  public ConsistentHashing(int numberOfReplicas) {
    this.numberOfReplicas = numberOfReplicas;
  }

  /**
   * Adds a server to the consistent hashing ring by generating replicas and hashing positions.
   * @param serverName The name or identifier of the server to be added.
   */
  public void addServer(String serverName) {
    for (int i = 0; i < numberOfReplicas; i++) {
      int hash = (serverName + i).hashCode();
      ring.put(hash, serverName);
    }
  }

  /**
   * Removes a server from the consistent hashing ring by removing its replicas.
   * @param serverName The name or identifier of the server to be removed.
   */
  public void removeServer(String serverName) {
    for (int i = 0; i < numberOfReplicas; i++) {
      int hash = (serverName + i).hashCode();
      ring.remove(hash);
    }
  }

  /**
   * Retrieves the server associated with a given key using consistent hashing.
   * @param key The key used to determine the associated server.
   * @return The server associated with the given key.
   */
  public String getServer(String key) {
    if (ring.isEmpty()) {
      System.out.println("There is no available servers currently.");
      return null;
    }
    int hash = key.hashCode();
    System.out.println("Key: " + key + " Hash: " + String.valueOf(hash));
    SortedMap<Integer, String> tailMap = ring.tailMap(hash);
    if (tailMap.isEmpty()) {
      return ring.get(ring.firstKey());
    }

    return tailMap.get(tailMap.firstKey());
  }

  public static void main(String[] args) {
    ConsistentHashing consistentHashing = new ConsistentHashing(2);

    // Add worker servers
    consistentHashing.addServer("112.55.66.68");
    consistentHashing.addServer("166.22.23.36");

    // Simulate distributing traffic
    String[] keys = {"Key1", "Key2", "SomethingDifferent", "AnotherTesting", "RandomTesting"};

    for (String key : keys) {
      String server = consistentHashing.getServer(key);
      System.out.println("Key: " + key + " -> Server: " + server);

      // then we can send the work to the selected server
    }
  }
}

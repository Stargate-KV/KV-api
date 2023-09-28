package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.enterprise.context.ApplicationScoped;
import org.stargate.rest.json.KVResponse;


/* QUESTIONS:
 * Dirty is not useful now because we update everything to cassandra first?
 * It seems no write-back is needed this time.
 * How to delete the locks? When deleting element from cache, the lock is acquired.
 * How to update the value in cache if the Cassandra got updated by other nodes? **\
 * 1. broadcast to all other cache service before return confirmation to client
 * 2. periadically check cloud (trade-off consistency + avability)
 * 3. each read check cloud (optimized network io)
 */
@ApplicationScoped
public class KVCache {

    private final int maxSlots;
    private final List<KVCacheSlot> cacheSlots; // List of pre-allocated slots
    private final List<Lock> locks; // List of pre-allocated Locks
    private final Queue<Integer> freeList; // Queue of available slot indices
    private final Map<Integer, Integer> hashToIndexMap; // Map of hash to index
    private final Queue<Integer> fifoOrder; // To implement FIFO eviction, elements are hash values
    

    public KVCache() {
        this.maxSlots = 10; // default to 10 at test stage
        this.fifoOrder = new LinkedList<>();
        this.cacheSlots = IntStream.range(0, maxSlots)
            .mapToObj(i -> new KVCacheSlot(null, null, null, null, null, false))
            .collect(Collectors.toList()); // Initialize all slots with 'used' set to false
        this.freeList = new LinkedList<>(IntStream.range(0, maxSlots).boxed().collect(Collectors.toList())); // All indices are initially free
        this.locks = IntStream.range(0, maxSlots).mapToObj(i -> new ReentrantLock()).collect(Collectors.toList()); // Initialize lock objects
        this.hashToIndexMap = new HashMap<>(); // Initialize hash to index mapping
    }

    public KVCacheSlot read(String key, String keyspace, String table) {
        // No lock for read currently
        int hash = computeHash(key, keyspace, table);
        int index = hashToIndexMap.getOrDefault(hash, -1);
        if (index != -1) {
            KVCacheSlot slot = cacheSlots.get(index);
            return slot.isUsed() ? slot : null;
        }
        printCache();
        return null;
    }

    public KVResponse delete(String key, String keyspace, String table) {
        int hash = computeHash(key, keyspace, table);
        synchronized (hashToIndexMap) {
            // sync block to ensure that the index we get here is correct one and does not modified by others
            int index = hashToIndexMap.getOrDefault(hash, -1);

            if (index != -1) {
                Lock lock = locks.get(index);
                lock.lock();
                try {
                    KVCacheSlot slot = cacheSlots.get(index);
                    slot.setUsed(false);
                    putInFreeQueue(index); // Return the index to the free list
                    removehashToIndexMap(hash); // Remove hash to index mapping
                } finally {
                    lock.unlock();
                }
                printCache();
                return new KVResponse(201, "The key of '" + key + "' has been deleted successfully.");
            } else {
                return new KVResponse(404, "The key of '" + key + "' not found.");
            }
        }
    }

    public KVResponse createOrUpdate(String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
        int hash = computeHash(key, keyspace, table);
        int index;
        Lock lock;
        synchronized (hashToIndexMap) {
            index = hashToIndexMap.getOrDefault(hash, -1);
    
            if (index != -1) {
                // Update
                lock = locks.get(index);
                lock.lock();
                try {
                    KVCacheSlot slot = cacheSlots.get(index);
                    assert slot.isUsed();
                    
                    slot.setValue(value);
                    slot.setValueType(valueType);
                    
                } finally {
                    lock.unlock();
                }
                printCache();
                return new KVResponse(201, "The key value pair '" + key + ":" + value + "' has been updated in cache successfully.");
            } else {
                // Create new entry
                synchronized (freeList) {
                    if (!freeList.isEmpty()) {
                        index = freeList.poll(); // Get a free slot index
                    }
                    else {
                        // Handle cache full - eviction logic
                        int oldestHash = fifoOrder.poll(); // Get the oldest hash
                        index = hashToIndexMap.remove(oldestHash); // Remove the oldest hash and get its index
                    }
                }
                hashToIndexMap.put(hash, index);

                lock = locks.get(index);
                lock.lock();

                try {
                    KVCacheSlot cacheslot = cacheSlots.get(index);
                    // Now, you can reuse the evicted slot for the new key-value pair
                    cacheslot.setUsed(true);
                    cacheslot.setValue(value);
                    cacheslot.setKey(key);
                    cacheslot.setKeyspace(keyspace);
                    cacheslot.setTable(table);
                    cacheslot.setValueType(valueType);
                    
                    fifoOrder.offer(hash); // Add new hash to fifo order
                } finally {
                    lock.unlock();
                }
                printCache();
                return new KVResponse(201, "The key value pair '" + key + ":" + value + "' has been inserted into cache, evicting an old entry.");
            }
        }
    }

    
    private int computeHash(String key, String keyspace, String table) {
        return Objects.hash(key, keyspace, table);
    }

    private void putInhashToIndexMap(int hash, int index) {
        synchronized (hashToIndexMap) {
            hashToIndexMap.put(hash, index);
        }
    }

    private void removehashToIndexMap(int hash) {
        synchronized (hashToIndexMap) {
            hashToIndexMap.remove(hash);
        }
    }

    private void putInFreeQueue(int index) {
        synchronized (freeList) {
            freeList.offer(index);
        }
    }

    private void removeFreeQueue(int index) {
        synchronized (freeList) {
            freeList.poll();
        }
    }

    public void printCache() {
        // Print cacheSlots List
        System.out.println("cacheSlots: ");
        for (KVCacheSlot slot : cacheSlots) {
            System.out.println(slot);
        }
    
        // Print freeList Queue
        System.out.println("freeList: " + freeList);
    
        // Print hashToIndexMap Map
        System.out.println("hashToIndexMap: ");
        for (Map.Entry<Integer, Integer> entry : hashToIndexMap.entrySet()) {
            System.out.println("Hash: " + entry.getKey() + ", Index: " + entry.getValue());
        }
    
        // Print fifoOrder Queue
        System.out.println("fifoOrder: " + fifoOrder);
    }
    
}

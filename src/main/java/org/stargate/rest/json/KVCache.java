package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.enterprise.context.ApplicationScoped;
import org.stargate.rest.json.KVResponse;


/* QUESTIONS:
 * Implement the LRU cache logic
 * We have the index i, but when we are processing, the element in this index might be 
 * deleted (evicted) already! Or replaced with another value!
 * One way is to store the hash value inside, check first whenever we need to process.
 * But the last update wins this case.
 * 
 * How to update the value in cache if the Cassandra got updated by other nodes? **\
 * 1. broadcast to all other cache service before return confirmation to client
 * 2. periadically check cloud (trade-off consistency + avability)
 * 3. each read check cloud (optimized network io)
 */
@ApplicationScoped
public class KVCache {

    // This is the LRU version of implementation
    private final int maxSlots;
    private final List<KVCacheSlot> cacheSlots; // List of pre-allocated slots
    private final List<Lock> locks; // List of pre-allocated Locks
    private final Queue<Integer> freeList; // Queue of available slot indices
    private final Map<Integer, Integer> hashToIndexMap; // Map of hash to index
    private final LinkedHashMap<Integer, Boolean> lruOrder; // To implement FIFO eviction, elements are hash values
    

    public KVCache() {
        long freeMemory = Runtime.getRuntime().freeMemory();
        this.maxSlots = (int)(freeMemory * 0.8 / (128 * 1024)); // Allocate 80% of the memory, max size of slots is 128KB
        this.lruOrder = new LinkedHashMap<Integer, Boolean>(maxSlots, 1.0f, true) {
            protected boolean removeEldestEntry(Map.Entry<Integer, Boolean> eldest) {
                return false; // Avoid automatic removal, since managing size externally
            }
        };
        this.cacheSlots = IntStream.range(0, maxSlots)
            .mapToObj(i -> new KVCacheSlot(null, null, null, null, null, false, -1))
            .collect(Collectors.toList()); // Initialize all slots with 'used' set to false
        this.freeList = new LinkedList<>(IntStream.range(0, maxSlots).boxed().collect(Collectors.toList())); // All indices are initially free
        this.locks = IntStream.range(0, maxSlots).mapToObj(i -> new ReentrantLock()).collect(Collectors.toList()); // Initialize lock objects
        this.hashToIndexMap = new ConcurrentHashMap<>(); // Initialize hash to index mapping
        
        // long totalMemory = Runtime.getRuntime().totalMemory();
        // long usedMemory = totalMemory - freeMemory;
        System.out.println("The freememory size: " + freeMemory + ". The number of slots we allocated: " + this.maxSlots);
    }

    public KVCacheSlot read(String key, String keyspace, String table) {
        int hash = computeHash(key, keyspace, table);
        int index = hashToIndexMap.getOrDefault(hash, -1);
        if (index != -1) {
            // Might need to improve to read lock afterwards
            KVCacheSlot slot = cacheSlots.get(index);
            // Check whether the hashvalue matches, if not, the value already got evicted.
            int slotHashvalue = slot.getHashvalue();
            if (slotHashvalue != hash) {
                return null;
            }
            synchronized (lruOrder) {
                lruOrder.put(hash, true); // Update LRU order
            }
            printCache();
            return slot.isUsed() ? slot : null;
        }
        
        return null;
    }

    public KVResponse delete(String key, String keyspace, String table) {
        int hash = computeHash(key, keyspace, table);
        int index = hashToIndexMap.getOrDefault(hash, -1);

        if (index != -1) {
            Lock lock = locks.get(index);
            lock.lock();
            try {
                KVCacheSlot slot = cacheSlots.get(index);
                int slotHashvalue = slot.getHashvalue();
                // Only proceed if hash matches
                if (slotHashvalue != hash) {
                    lock.unlock();
                    return new KVResponse(404, "The key of '" + key + "' not found.");
                }
                slot.setUsed(false);
                putInFreeQueue(index);
                removehashToIndexMap(hash); // Remove hash to index mapping
                synchronized (lruOrder) {
                    lruOrder.remove(hash); // Remove the hash from the LRU order
                }
            } finally {
                lock.unlock();
            }
            printCache();
            return new KVResponse(201, "The key of '" + key + "' has been deleted successfully.");
        } else {
            return new KVResponse(404, "The key of '" + key + "' not found.");
        }
    }

    public KVResponse create(String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
        int hash = computeHash(key, keyspace, table);
        int index;
        Lock lock;

        index = hashToIndexMap.getOrDefault(hash, -1);
        if (index != -1) { // Go to update in this case
            return update(key, value, keyspace, table, valueType);
        }

        assert index == -1;
        synchronized (freeList) {
            if (!freeList.isEmpty()) {
                index = freeList.poll(); // Get a free slot index
            }
            else {
                // Handle cache full - eviction logic
                synchronized (lruOrder) {
                    Iterator<Integer> it = lruOrder.keySet().iterator();
                    if (it.hasNext()) {
                        int oldestHash = it.next(); // Get the oldest hash
                        it.remove();
                        index = hashToIndexMap.remove(oldestHash); // Remove the oldest hash and get its index
                    } else {
                        return new KVResponse(500, "ERROR: The lruOrder does not have any value inside for eviction!");
                    }
                }
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
            cacheslot.setHashvalue(hash);
            
            synchronized (lruOrder) {
                lruOrder.put(hash, true); // Add new hash to LRU order
            }

        } finally {
            lock.unlock();
        }

        printCache();
        return new KVResponse(201, "The key value pair '" + key + ":" + value + "' has been inserted into cache, evicting an old entry.");
    }

    public KVResponse update(String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
        int hash = computeHash(key, keyspace, table);
        int index;
        Lock lock;

        index = hashToIndexMap.getOrDefault(hash, -1);
        if (index == -1) { // Go to create in this case
            return create(key, value, keyspace, table, valueType);
        }

        assert index != -1;

        lock = locks.get(index);
        lock.lock();
        try {
            KVCacheSlot slot = cacheSlots.get(index);
            assert slot.isUsed();
            int slotHashvalue = slot.getHashvalue();
            // If hash does not match, this one got evicted, go to create logic
            if (slotHashvalue != hash) {
                lock.unlock();
                return create(key, value, keyspace, table, valueType);
            }
            slot.setValue(value);
            slot.setValueType(valueType);
            
        } finally {
            lock.unlock();
        }

        synchronized (lruOrder) {
            lruOrder.put(hash, true); // Update LRU order
        }
        printCache();
        return new KVResponse(201, "The key value pair '" + key + ":" + value + "' has been updated in cache successfully.");
    }

    
    private int computeHash(String key, String keyspace, String table) {
        return Objects.hash(key, keyspace, table);
    }

    private void removehashToIndexMap(int hash) {
        hashToIndexMap.remove(hash);
    }

    private void putInFreeQueue(int index) {
        synchronized (freeList) {
            freeList.offer(index);
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
    
        // Print lruOrder Queue
        System.out.println("lruOrder: " + lruOrder);
    }
    
}

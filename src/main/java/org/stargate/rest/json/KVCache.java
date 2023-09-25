package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.enterprise.context.ApplicationScoped;
import org.stargate.rest.json.KVResponse;

/* QUESTIONS:
 * Dirty is not useful now because we update everything to cassandra first?
 * It seems no write-back is needed this time.
 * How to delete the locks? When deleting element from cache, the lock is acquired.
 * How to update the value in cache if the Cassandra got updated by other nodes? **
 */
@ApplicationScoped
public class KVCache {

    private final int maxSlots;
    private int usedSlots;
    private final Map<Integer, KVCacheSlot> slots;
    private final Map<Integer, Lock> lockMap;
    private final LinkedList<Integer> fifoOrder; // To implement FIFO eviction

    public KVCache() {
        this.maxSlots = 10; // default to 10 at test stage
        this.usedSlots = 0;
        this.slots = new HashMap<>();
        this.lockMap = new HashMap<>();
        this.fifoOrder = new LinkedList<>();
    }

    public KVCacheSlot read(String key, String keyspace, String table) {
        // No lock for read currently
        int hash = computeHash(key, keyspace, table);

        return slots.getOrDefault(hash, null);
    }

    public KVResponse delete(String key, String keyspace, String table) {
        int hash = computeHash(key, keyspace, table);
        Lock lock = getLockForHash(hash);

        lock.lock();
        try {
            // slots.remove(hash);
            // usedSlots--;
            // fifoOrder.remove((Integer) hash);
            // Only set this value to true instead of removing
            KVCacheSlot slot = slots.get(hash);
            slot.setTombstone(true);
        } finally {
            lock.unlock();
        }
        return new KVResponse(201, "The key of '" + key + " has been deleted successfully.");
    }

    public KVResponse createOrUpdate(String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
        int hash = computeHash(key, keyspace, table);
        
        if (usedSlots >= maxSlots && !slots.containsKey(hash)) {
            // eviction & insert
            int oldestHash = fifoOrder.peekFirst();
            acquireLocksInOrder(hash, oldestHash);
            try {
                if (!slots.containsKey(hash)) {
                    evictOldest();
                }
                internalCreateOrUpdate(hash, key, value, keyspace, table, valueType);
            } finally {
                releaseLocksInOrder(hash, oldestHash);
            }
        } else {
            Lock lock = getLockForHash(hash);
            lock.lock();
            try {
                internalCreateOrUpdate(hash, key, value, keyspace, table, valueType);
            } finally {
                lock.unlock();
            }
        }
        return new KVResponse(201, "The key value pair '" + key + ":" + value + "' has been inserted into cache successfully.");
    }

    private void internalCreateOrUpdate(int hash, String key, JsonNode value, String keyspace, String table, KVDataType valueType) {
        if (slots.containsKey(hash)) {
            // Update
            KVCacheSlot slot = slots.get(hash);
            if (slot.getValue() == value && slot.getValueType() == valueType && slot.getTombstone()==false) {
                // No need to update
                return;
            }
            slot.setValue(value);
            slot.setValueType(valueType);
            // slot.setDirty(true);
            // In case it is a deleted entry, then mark it as active
            slot.setTombstone(false);
            
            // remove and insert to the top of queue
            fifoOrder.remove((Integer) hash); // Explicitly cast to avoid confusion with index-based removal
        } else {
            // Create
            slots.put(hash, new KVCacheSlot(key, value, keyspace, table, valueType, false, false));
            usedSlots++;
        }
        
        fifoOrder.add(hash);
    }
    
    private int computeHash(String key, String keyspace, String table) {
        return Objects.hash(key, keyspace, table);
    }

    private Lock getLockForHash(int hash) {
        return lockMap.computeIfAbsent(hash, key -> new ReentrantLock());
    }

    private void evictOldest() {
        int oldestHash = fifoOrder.removeFirst();
        slots.remove(oldestHash);
        usedSlots--;
    }

    private void acquireLocksInOrder(int hash1, int hash2) {
        if (hash1 < hash2) {
            getLockForHash(hash1).lock();
            getLockForHash(hash2).lock();
        } else {
            getLockForHash(hash2).lock();
            getLockForHash(hash1).lock();
        }
    }
    
    private void releaseLocksInOrder(int hash1, int hash2) {
        getLockForHash(hash1).unlock();
        getLockForHash(hash2).unlock();
    }
}

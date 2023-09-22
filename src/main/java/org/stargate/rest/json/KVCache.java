package org.stargate.rest.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.enterprise.context.ApplicationScoped;
import org.stargate.rest.json.KVResponse;

@ApplicationScoped
public class KVCache {

    private final int maxSlots;
    private int usedSlots;
    private final Map<Integer, KVCacheSlot> slots;
    private final Map<Integer, Lock> lockMap;
    private final LinkedList<Integer> fifoOrder; // To implement FIFO eviction

    public KVCache() {
        this.maxSlots = 10;
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
            slots.remove(hash);
            usedSlots--;
            fifoOrder.remove((Integer) hash);
        } finally {
            lock.unlock();
        }
        return new KVResponse(201, "The key of '" + key + " has been deleted from cache successfully.");
    }

    public KVResponse createOrUpdate(String key, JsonNode value, String keyspace, String table, KVDataType valueType, boolean dirty) {
        int hash = computeHash(key, keyspace, table);
        
        if (usedSlots >= maxSlots && !slots.containsKey(hash)) {
            // eviction & insert
            int oldestHash = fifoOrder.peekFirst();
            acquireLocksInOrder(hash, oldestHash);
            try {
                if (!slots.containsKey(hash)) {
                    evictOldest();
                }
                internalCreateOrUpdate(hash, key, value, keyspace, table, valueType, dirty);
            } finally {
                releaseLocksInOrder(hash, oldestHash);
            }
        } else {
            Lock lock = getLockForHash(hash);
            lock.lock();
            try {
                internalCreateOrUpdate(hash, key, value, keyspace, table, valueType, dirty);
            } finally {
                lock.unlock();
            }
        }
        return new KVResponse(201, "The key value pair '" + key + ":" + value + "' has been inserted into cache successfully.");
    }

    private void internalCreateOrUpdate(int hash, String key, JsonNode value, String keyspace, String table, KVDataType valueType, boolean dirty) {
        if (slots.containsKey(hash)) {
            // Update
            KVCacheSlot slot = slots.get(hash);
            if (slot.getValue() == value && slot.getValueType() == valueType) {
                // No need to update
                return;
            }
            slot.setValue(value);
            slot.setValueType(valueType);
            slot.setDirty(true);
            
            fifoOrder.remove((Integer) hash); // Explicitly cast to avoid confusion with index-based removal
        } else {
            // Create
            slots.put(hash, new KVCacheSlot(key, value, keyspace, table, valueType, dirty, false));
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

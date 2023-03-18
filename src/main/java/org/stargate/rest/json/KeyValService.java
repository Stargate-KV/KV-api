package org.stargate.rest.json;

import java.util.HashMap;
import java.util.Map;


public class KeyValService {
	Map<Integer, Map<String, String>> internal_cache_map;
	Integer id_counter;
	public KeyValService() {
		internal_cache_map = new HashMap<>();
		id_counter = 0;
	}
	public synchronized int createDatabase() {
		int res = id_counter++;
		internal_cache_map.put(res, new HashMap<>());
		return res;
	}
	public void putKeyVal(int db_id, String key, String val, boolean update) throws KvstoreException {
		synchronized(internal_cache_map) {
			if (!internal_cache_map.containsKey(db_id)) {
				throw new KvstoreException(404, "db not found");
			}
			if (internal_cache_map.get(db_id).containsKey(key)) {
				if (!update) {
					throw new KvstoreException(409, "key already exist");
				}
			}else {
				if (update) {
					throw new KvstoreException(404, "key not found");
				}
			}
			internal_cache_map.get(db_id).put(key,  val);
		}
	}
	
	public String getKey(int db_id, String key) throws KvstoreException {
		synchronized(internal_cache_map) {
			if (!internal_cache_map.containsKey(db_id)) {
				throw new KvstoreException(404, "db not found");
			}
			if (!internal_cache_map.get(db_id).containsKey(key)) {
				throw new KvstoreException(404, "key not found");
			}
			return internal_cache_map.get(db_id).get(key);
		}
	}
	public void deleteKey(int db_id, String key) throws KvstoreException {
		synchronized(internal_cache_map) {
			if (!internal_cache_map.containsKey(db_id)) {
				throw new KvstoreException(404, "db not found");
			}

			internal_cache_map.get(db_id).remove(key);
			
		}
	}
	public void deleteDB(int db_id) {
		synchronized(internal_cache_map) {
			if (internal_cache_map.containsKey(db_id)) {
				internal_cache_map.remove(db_id);
			}
			
		}
	}
}

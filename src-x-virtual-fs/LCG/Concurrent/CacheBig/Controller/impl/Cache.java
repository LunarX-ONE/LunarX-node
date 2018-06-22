package LCG.Concurrent.CacheBig.Controller.impl;

 
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class Cache<KEY,VALUE> {

	private final Lock lock = new ReentrantLock();
	private final int max_capacity;
	private final Map<KEY,VALUE> cached_obj;
 

	public Cache(int init_max_capacity) {
		this.max_capacity = init_max_capacity;
		this.cached_obj = new ConcurrentHashMap<KEY,VALUE>(max_capacity);
		 
	}

	public VALUE get(KEY k) {
		VALUE v = this.cached_obj.get(k);
		if (v == null) {
			lock.lock();
			try{
				//to do: get from IO Stream
			}finally{
				lock.unlock();
			}
			if (v != null) {
				this.cached_obj.put(k, v);
			}
		}
		return v;
	}

	public void put(KEY k, VALUE v) {
		if (this.cached_obj.size() >= max_capacity) {
			lock.lock();
			try{
				//to do: flush all cached objects to disk
			}finally{
				lock.unlock();
			}
			this.cached_obj.clear();
		}	
		this.cached_obj.put(k, v);
	}
}
	

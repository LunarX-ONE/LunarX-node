/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author LunarDB team, feiben@lunarion.com
  * The contents of this file are subject to the Lunarion Public License Version 1.0
  * ("License"); You may not use this file except in compliance with the License
  * The Original Code is:  Lunar NoSQL Database source code 
  * The Lunar NoSQL Database source code is based on Lunarion Cloud Platform(solution.lunarion.com)
  * The Initial Developer of the Original Code is the development team at Lunarion.com.
  * Portions created by lunarion are Copyright (C) lunarion.
  * All Rights Reserved.
  *******************************************************************************
 * 
 */

package LunarX.Memory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import LCG.Concurrent.CacheBig.CacheLRU;
import LCG.Concurrent.CacheBig.CacheLRUConcurrent;
import LCG.Concurrent.CacheBig.Controller.MemPoolInterface;
import LCG.FSystem.Def.DBFSProperties;
import LunarX.Node.Utils.HashCadidates;
import Lunarion.SE.AtomicStructure.RowEntry;
import Lunarion.SE.HashTable.Stores.StoreRowEntries; 

/*
 * This is for concurrent visit, for multiple threads read and write.
 * It applies block level locker.
 */

public class RowKeyCache extends Cache{

	 
	private final CacheLRUConcurrent<String, RowEntry> key_in_use;
	//private final CacheLRUConcurrent<Long, RowEntry> key_in_use;
	private final LinkedHashMap<String, RowEntry> dirty_entries;
	 private AtomicLong on_dirty_locked = new AtomicLong(0);
	      
	
	private final int cache_capacity;
	private final int concurrency_level;
	 
 
	protected int key_count ;

	public RowKeyCache(int _key_count, int concurrent_level) {
		
		System.out.println("Memory Cache for Keys is loading now......");
		key_count = Math.max(concurrent_level * 3,  Math.min(Integer.MAX_VALUE, _key_count));
		
		//int _extra_room = (int)(DBFSProperties.concurrent_level * 2 );
		//key_count += _extra_room;
		int how_many_in_cache = key_count;// - extra_room;
		// <key_string, RowEntry> 
		key_in_use = new CacheLRUConcurrent<String, RowEntry>(how_many_in_cache, concurrent_level);
		cache_capacity = key_in_use.getCapacity();
		concurrency_level = key_in_use.getConcurrencyLevel(); 

		this.key_in_use.clear();
		
		dirty_entries = new LinkedHashMap<String, RowEntry>();
		this.init();
	} 

	public int getConcurrencyLevel() {
		return this.key_in_use.getConcurrencyLevel();
	}

	public int getCapacity() {
		return this.cache_capacity;
	}

	public CacheLRU getCacheCongruent(int cache_index)
	{
		return this.key_in_use.getCacheSeg(cache_index);
	}
	
	 
	Iterator<Entry<String, RowEntry>> getDirtyEntries()
	{
		return this.dirty_entries.entrySet().iterator();
	}
	
	public void flushDirtyEntries(StoreRowEntries outer_storage)
	{
		on_dirty_locked.getAndIncrement();
		
		Iterator<Entry<String, RowEntry>> it = (Iterator<Entry<String, RowEntry>>)dirty_entries.entrySet().iterator();
		//int count = dirty_entries.size();
		//System.out.println(count);
		if (it != null) 
		{
			while (it.hasNext()) 
			{  
				java.util.Map.Entry<String, RowEntry> e = it.next();
		        if (e != null && e.getValue() != null ) 
		        { 
		        	RowEntry t_obj = e.getValue();   
		        	try {
						outer_storage.write(t_obj); 
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
		        }
		    }
		}
		dirty_entries.clear();
		on_dirty_locked.getAndDecrement();
	}
	 
	
	public void addToCache( RowEntry row_entry, StoreRowEntries outer_storage)   {
		if(shutdown_called.get())
			return;
		
		critical_reference.getAndIncrement();
		
		String key_str;
		try {
			key_str = row_entry.s_key_string.Get();
		
			synchronized (this.key_in_use.inWhichSeg(key_str)) { 
			//long hash_key = HashCadidates.hashJVM(key_str);
			//synchronized (this.key_in_use.inWhichSeg(hash_key)) { 
				this.key_in_use.put(key_str, row_entry);
				if (!this.key_in_use.inWhichSeg(key_str).getRemoved().isEmpty()) { 
					flushAndRelease(key_str, this.key_in_use.inWhichSeg(key_str).getRemoved(), outer_storage);
				}  
			}
			if(row_entry.isDirt())
			{ 
				while(on_dirty_locked.get()>0)
					;
				this.dirty_entries.put(key_str, row_entry); 
			}
		} catch (UnsupportedEncodingException e) {
			//critical_reference.getAndDecrement();
			e.printStackTrace();
		} finally
		{
			critical_reference.getAndDecrement();
		}
	}

	private final void flushAndRelease(String key, BlockingQueue<RowEntry> entry_waiting_for_free, StoreRowEntries outer_storage) {
		 
		while (!entry_waiting_for_free.isEmpty()) {
			RowEntry removed = null;
			try {
				removed = entry_waiting_for_free.take();
				if (!removed.isDirt()) { 
					continue;
				} 
				outer_storage.write(removed); 
				while(on_dirty_locked.get()>0)
					;
				this.dirty_entries.remove(removed.s_key_string.Get());
				 
				continue;
			}  
			catch (IOException e) {
				System.out.println("[Error]: failed to flush dirt entry to outer storage @RowKeyCache.flushAndRelease(...).");
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		 
	}
 

	public RowEntry getObj(String key_string) {
		if (key_string == null)
			return null;

		if (!shutdown_called.get()) {
			//long key_hash = HashCadidates.hashJVM(key_string);
			
			//RowEntry t_entry = this.key_in_use.get(key_hash);
			RowEntry t_entry = this.key_in_use.get(key_string);
			return t_entry;  
		}
		throw new IllegalStateException("Row Key Memory Pool is already shutdown");
	}

	 

	public boolean tryReleaseMemory(RowEntry entry_tobe_release ) { 
			return false; 
	}

	/*
	 * It is unsafe. the block that has been forceReleased looses its data
	 * forever, if it has not write to outer storage in time
	 */
	public boolean forceReleaseMemory(RowEntry entry_tobe_release) {
		 
		return false;
	} 
	 
	public int capacity() {
		return this.cache_capacity;
	}
 
 

	public void shutDown() {
		System.out.println("Memory Cache for Keys is shutting down now......");
 
		 
		/*
		 * then no other thread can let this object do a new operation, 
		 * in other words, critical_reference will not increase.
		 */
		if(!shutdown_called.get())
			shutdown_called.set(true);
		
		/*
		 * waiting the running tasks finish.
		 */
		while(critical_reference.get()>0)
			System.out.println("waiting critical task finish. @RowKeyCache.shutDown()");

		this.key_in_use.clear(); 
		this.dirty_entries.clear();
		// this.thread_executor.shutdown();
		System.out.println("Exited.");
	}

}

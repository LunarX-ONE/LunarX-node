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
 
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import LCG.Concurrent.CacheBig.CacheLRU;
import LCG.Concurrent.CacheBig.CacheLRUConcurrent;
import LCG.FSystem.Def.DBFSProperties;
import LCG.MemoryNative64.ByteArray;
import LCG.MemoryNative64.LunarMMU;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
 
/*
 * This is for concurrent visit, for multiple threads read and write.
 * It applies block level locker.
 */

public class TableMemoryStore extends Cache{

	
	/*
	 * @Integer, the record id,
	 * @ByteArray, the record string that in off-heap memory
	 */
	private final CacheLRUConcurrent<Integer, ByteArray> hot_records;
	 
	protected final int cache_capacity; 
	 
	
	protected int rec_count ;

	private LunarMMU mmu;
	
	public TableMemoryStore(int _rec_count, int concurrent_level) {
		System.out.println("Memory Table for records is loading now......");

		rec_count = Math.max(concurrent_level * 3,  Math.min(Integer.MAX_VALUE, _rec_count));
	 	 
		int how_many_in_cache = rec_count; 
		// <key_string, RowEntry> 
		hot_records = new CacheLRUConcurrent<Integer, ByteArray>(how_many_in_cache, concurrent_level);
		cache_capacity = hot_records.getCapacity();
		 
		mmu = new LunarMMU();
		
		this.hot_records.clear();
		this.init();
		
	}

	

	public int getConcurrencyLevel() {
		return this.hot_records.getConcurrencyLevel();
	}

	public int getCapacity() {
		return this.cache_capacity;
	}

	public void addToCache( Record32KBytes record ) { 
		if(shutdown_called.get())
			return;
		
		critical_reference.getAndIncrement();
		
		synchronized (this.hot_records.inWhichSeg(record.getID())) {
			ByteArray ba = this.hot_records.get(record.getID());
			try
			{ 
				if(ba != null)
					ba.releaseNative(); 
				
				try {
					ba = new ByteArray(mmu, record.recLength()); 
					//System.err.println("allocated one obj");
					
					record.Write(ba);
					} 
				catch (Exception e) {
					ba.releaseNative(); 
					critical_reference.getAndDecrement();
					return;
				}
			
				
			}
			finally
			{
				/*
				 * the final segment may be interrupted by Ctrl+C, 
				 * when user closes process on cmd console.
				 * So here use the atomic critical_reference to 
				 * tell shutDown()(or other functions which may interrupt this) 
				 * waiting for a moment.
				 */
				this.hot_records.put(record.getID(), ba); 
				//System.err.println("put one obj");
				
				if (!this.hot_records.inWhichSeg(record.getID()).getRemoved().isEmpty()) 
				{ 
					releaseNative(this.hot_records.inWhichSeg(record.getID()).getRemoved() );
					//System.err.println("released one old obj");
				}
				
				critical_reference.getAndDecrement();
			} 
		}
	}
	
	public void releaseObj(int record_id)
	{
		if(shutdown_called.get())
			return;
	
		ByteArray ba = this.hot_records.get(record_id);
		if(ba != null)
			ba.releaseNative();
		
		this.hot_records.remove(record_id);
		if (!this.hot_records.inWhichSeg(record_id).getRemoved().isEmpty()) 
		{ 
			releaseNative(this.hot_records.inWhichSeg(record_id).getRemoved() );
		}
	}

	private final void releaseNative(BlockingQueue<ByteArray> rec_waiting_for_free )  
	{
		while (!rec_waiting_for_free.isEmpty()) {
			ByteArray removed = null;

			try {
				removed = rec_waiting_for_free.take();
				removed.releaseNative();   
			} catch (InterruptedException e) {
				System.err.println("[ERROR]: severe error@TableMemoryStore.releaseNative(...): thread interrupted when releasing native memory");
				e.printStackTrace();
			} 
		}
	}
	
	private void releaseNativeCacheSeg(CacheLRU<Integer, ByteArray> cache_seg)
	{
		Iterator<Entry<Integer, ByteArray>> it = (Iterator<Entry<Integer, ByteArray>>)cache_seg.KeyValueIterator();
		if (it != null) 
		{
			while (it.hasNext()) 
			{  
				java.util.Map.Entry<Integer, ByteArray> e = it.next();
				if (e != null && e.getValue() != null ) 
				{ 
					ByteArray t_obj = e.getValue();  
					t_obj.releaseNative();
					  
				} 
			}
		}
	}
 

	public Record32KBytes getObj(int rec_id) {
		if (rec_id < 0)
			return null;

		
		if (!shutdown_called.get()) {
			ByteArray ba = this.hot_records.get(rec_id);
			if( ba == null)
				return null; 
		 
			Record32KBytes rec = new Record32KBytes(rec_id);
			try {
				rec.Read(ba);
			} catch (Exception e) {
				return null;
			} 
			
			return rec;  
		}
		throw new IllegalStateException("Record Memory Table is already shutdown");
	} 
	 
	public int capacity() {
		return this.cache_capacity;
	} 
	
	public void shutDown() {
		System.out.println("Memory Table is shutting down now......");
		
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
			System.out.println("waiting critical task finish. @TableMemoryStore.shutDown()");

		int con_level = this.hot_records.getConcurrencyLevel();
		for(int i =0; i< con_level; i++)
		{
			CacheLRU<Integer, ByteArray> cache_seg = this.hot_records.getCacheSeg(i);
			synchronized(cache_seg)
			{
				releaseNativeCacheSeg(cache_seg); 
				//System.out.println("all native objects released.");
				 
				BlockingQueue<ByteArray> removed_queue = cache_seg.getRemoved();
				while (!removed_queue.isEmpty()) {
					ByteArray removed = null;

					try {
						removed = removed_queue.take();
						removed.releaseNative();   
						System.out.println("one garbage object removed.");
						
					} catch (InterruptedException e) {
						System.err.println("severe error@TableMemoryStore.shutDown(...): thread interrupted when releasing native memory");
						
						e.printStackTrace();
					} 
				}
			}
		}
		
		this.hot_records.clear();
		
		this.mmu.terminate(); 
		
		 
		System.out.println("Memory Table Exited.");
	}
	
 
	
		

}

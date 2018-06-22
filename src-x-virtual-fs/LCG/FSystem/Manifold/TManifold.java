/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contactor: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License.
 * The Original Code is:  LunarBase source code 
 * The LunarBase source code is managed by the development team at Lunarion.com.
 * The Initial Developer of the Original Code is the development team at Lunarion.com.
 * Portions created by lunarion are Copyright (C) lunarion.
 * All Rights Reserved.
 *******************************************************************************
 * 
 */

package LCG.FSystem.Manifold;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException; 
import java.util.Iterator; 
import java.util.Map.Entry;  
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import LCG.Concurrent.CacheBig.CacheLRU;
import LCG.Concurrent.CacheBig.Controller.MemPoolInterface;
import LCG.Concurrent.CacheBig.Controller.impl.BlockCache;
import LCG.Concurrent.CacheBig.Controller.impl.BlockCacheLazyAllocate;
import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.Concurrent.Tasks.ObjCollectorInPool;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.AtomicStructure.IFSObject;
import LCG.FSystem.Def.DBFSProperties;
import LCG.StorageEngin.IO.L0.IOInterface; 
import LCG.StorageEngin.Serializable.IObject;
 

abstract public class TManifold<T extends BlockSimple, POOL extends BlockCacheLazyAllocate> extends LFSBaseLayer{ 
    
 	final POOL _mem_pool;  
 	private final int dir_element_byte_len = 0; //2^0 = 1 byte for each dir element;
	private final int dir_element_len = 1;
	
	public TManifold(String _root_path, String _table_name, 
						String _mode, 
						int bufbitlen, 
						POOL _pool, 
						boolean native_mode,
						DBFSProperties _dbfs_prop_instance) throws IOException, FileNotFoundException {
		super(_root_path, _table_name, _mode, bufbitlen, native_mode, _dbfs_prop_instance); 
		_mem_pool = _pool;
		
		object_count = 0;
		 
		object_count = objects_dir.length() >> dir_element_byte_len;
		
		this.init();
	}
	
	public void init()
	{
		
	}
	
	public void createCache()
	{
		//int how_many = this.cache_capacity;
		if(this._mem_pool == null)
		{	
			System.err.println("[Error]: the memory has not been initilized yet and it is not accessible" );  
			return;
		}
		 
 
	}
	 
	public long getObjPos(T obj, int obj_id)
	{ 
		return obj_id*obj.sizeInBytes();
	} 
	 
	public void writeObj(T obj, long obj_id) throws IOException
	{
		if(obj.getID() != obj_id)
			return;
		
		obj.write(this);  
	}  
	
	protected void appendDir() throws IOException
	{
		synchronized(this.objects_dir)
		{  
			//this.objects_dir.seek(object_count);
			//this.objects_dir.write(DBFSProperties.unavailable);
			long obj_count_before_appending = this.objects_dir.appendDir(dir_element_len, 
										DBFSProperties.unavailable);
			if(object_count != obj_count_before_appending)
				System.err.println("[ERROR]: manifold and its directory has in consistent object count "
								+ " @TManifold.appendDir()");
		}
	}
	
	/*
	 * Only one thread can enter this function, 
	 * since this.getLatest is not thread Safes, and even if it is, 
	 * the combination of the appending operation is not.
	 * 
	 * This function just append the file with a new block size, 
	 * but not actually flush the block (with the biggest id as of now).
	 * 
	 * returns the id that evaluated via appending.
	 */ 
 
	/*
	protected synchronized int appendNewInDisk(T new_obj)  
	{   
		IOInterface io_latest = this.getLatest();
		 
		int new_length = (int) (io_latest.length() + new_obj.Size());
		if(new_length<=this.seg_file_length)
		{
			new_obj.setID(object_count);
			try
			{
				appendDir(); 
				object_count++;
				io_latest.setLength(new_length); 
				  
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				new_obj.setID(-1);
				return -1;
			}
			return new_obj.getID();
			 
		}
		else if(this.acquirDisk())
		{
			new_obj.setID(object_count);
			try
			{
				appendDir(); 
				object_count++;
				IOInterface io_new = this.spawn(); 
				synchronized (io_new)
				{
					io_new.setLength(new_obj.Size());
					//this.writeObj(new_obj, new_obj.getID());
				}
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				new_obj.setID(-1);
				return -1;
			}
			 
			return new_obj.getID();
		}
		else
		{
			 
			this._mem_pool.forceReleaseMemory(new_obj.getMemReference());
			 
			return -1;
		}  
	}  
	*/
 
	protected synchronized long appendNewInDisk(T new_obj) 
	{
		//if(appendObjInDisk(new_obj)==-1)
		if(appendObjInDisk(new_obj.sizeInBytes())==-1)
		{
			this._mem_pool.forceReleaseMemory(new_obj.getMemReference());
			/*
			 * since appendObjInDisk increases object_count by 1 
			 */
			object_count--;
			new_obj.setID(-1); 
			return -1;
		}
		else
		{
			/*
			 * since appendObjInDisk increases object_count by 1 
			 */
			new_obj.setID(object_count-1);
			return new_obj.getID();
		}
	}
	 
	private void FlushASeg(CacheLRU<Integer, T> one_seg) throws IOException
	{
		if(one_seg ==null)
			return;
		if(one_seg.size()==0)
			return;
		 
		one_seg.lockOuter();
		Iterator<Entry<Integer, T>> it = (Iterator<Entry<Integer, T>>)one_seg.KeyValueIterator();
		if (it != null) 
		{
			while (it.hasNext()) 
			{  
				java.util.Map.Entry<Integer, T> e = it.next();
		        if (e != null && e.getValue() != null && e.getValue() != null) 
		        { 
		        	T t_obj = e.getValue();  
		        	if(t_obj.isDirt())
		        	{
		        		this.writeObj(t_obj, t_obj.getID()); 
		        	} 
		        	//System.out.println("flush this cached block to disk : " + t_obj.s_obj_id);  
		        }
		    }
		}
		
		one_seg.unlockOuter();
		 
	}
	
	//just flush, do not clear the cache. 
	//Actually, has no privilege to clear a global cache.  
	public void flushCache() throws IOException
	{ 
		//flush all the cache to disk 
		//CacheLRU<Integer, T>[] all_segs = cache_congruent.GetAllSegs();
		int con_level = this._mem_pool.getConcurrencyLevel();
		for(int i =0; i< con_level; i++)
		{
			CacheLRU<Integer, T> cache_seg = this._mem_pool.getCacheCongruent(i);
			 
			FlushASeg(cache_seg); 
			 
		}  
		
		objects_dir.flush();
		this.flush();
	} 
	
	public long getObjCount()
	{
		return object_count; 
	}  
	
	public void shutdownPool()
	{
		if(this._mem_pool!=null)
		{
			this._mem_pool.shutDown();
		}
	}
	 
}

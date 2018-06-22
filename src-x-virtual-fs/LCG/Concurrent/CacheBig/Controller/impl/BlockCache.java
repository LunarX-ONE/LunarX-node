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

package LCG.Concurrent.CacheBig.Controller.impl;

 
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import LCG.Concurrent.CacheBig.CacheLRU;
import LCG.Concurrent.CacheBig.CacheLRUConcurrent;
import LCG.Concurrent.CacheBig.Controller.MemPoolInterface;
import LCG.Concurrent.Tasks.ObjCollectorInPool;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.AtomicStructure.IFSObject;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.FSystem.Manifold.TManifold;
 
 
/*
 * This is for concurrent visit, for multiple threads read and write.
 * It applies block level locker.
 */

public class BlockCache extends MemPoolInterface{

	//protected static final BlockSizes DEFAULT_BlOCK_SIZE = DBFSProperties.BlockSizes.block_4k; 
	//protected static final int DEFAULT_BLOCKS_COUNT = DBFSProperties.max_4k_blocks_in_cache;
	
    //Once the capacity initialized, never changes.
	//Acquiring a block in memory can only be done via its memory reference
	//private BlockMem[] g_obj_pool;
	protected final BlockSimple[] g_block_pool;
	 
	protected final CacheLRUConcurrent<Long, BlockSimple> blocks_in_use;
	protected final int cache_capacity;
	protected final int concurrency_level; 
	//private final ExecutorService thread_executor; 
	protected volatile boolean shutdown_called;
    
	  
	public final BlockingQueue<MemoryStrongReference> blocks_free_queue;
	protected long store_size; 
    protected int g_obj_count = 1 << 4 ;//DEFAULT_BLOCKS_COUNT;
    protected final BlockSizes blocks_size;
     
    protected BlockCache(BlockSizes _b_size, long _blocks_count
				, DBFSProperties dbfs_prop_instance, final boolean no_init ) 
    { 
    	System.out.println("[INFO]: Memory Pool with block size " + _b_size.getSize() +" bytes is loading now......");  
    	//this.g_block_pool = (BlockMem[])this.g_obj_pool; 
 
    	blocks_size = _b_size;
    	//blocks_count = (int) Math.min(Integer.MAX_VALUE, _blocks_count); 
    	
    	/*
    	 * set the minimum capacity of this cache system.
    	 * the meaning of minimum g_obj_count and extra room, 
    	 * shall be consulted at BucketCache.h, 
    	 * 
    	 */
    	g_obj_count = Math.max(dbfs_prop_instance.concurrent_level * 3, (int) Math.min(1<<31, _blocks_count));
		 

		int _extra_room = (int)(dbfs_prop_instance.concurrent_level * 2);
		g_obj_count += _extra_room;

    	
    	this.g_block_pool = new BlockSimple[g_obj_count];
    	blocks_free_queue = new LinkedBlockingQueue<MemoryStrongReference>(g_obj_count);
		
    	//<block id, block> 
        int how_many_in_cache = Math.max(1, g_obj_count - _extra_room); 
		blocks_in_use = new CacheLRUConcurrent<Long, BlockSimple>(how_many_in_cache, dbfs_prop_instance.concurrent_level);
		cache_capacity = blocks_in_use.getCapacity();
		concurrency_level = blocks_in_use.getConcurrencyLevel(); 
		 
        
        System.out.println("[INFO]: Memory Pool containing " + g_obj_count +" blocks, with each "+_b_size.getSize() +" bytes will be allocated when needed. ");  
    	
    }
    protected BlockCache(BlockSizes _b_size, long _blocks_count
    					, DBFSProperties dbfs_prop_instance ) { 
    	System.out.println("[INFO]: Memory Pool with block size " + _b_size.getSize() +" bytes is loading now......");  
    	//this.g_block_pool = (BlockMem[])this.g_obj_pool; 
 
    	blocks_size = _b_size;
    	//blocks_count = (int) Math.min(Integer.MAX_VALUE, _blocks_count); 
    	
    	/*
    	 * set the minimum capacity of this cache system.
    	 * the meaning of minimum g_obj_count and extra room, 
    	 * shall be consulted at BucketCache.h, 
    	 * 
    	 */
    	g_obj_count = Math.max(dbfs_prop_instance.concurrent_level * 3, (int) Math.min(1<<31, _blocks_count));
		 

		int _extra_room = (int)(dbfs_prop_instance.concurrent_level * 2);
		g_obj_count += _extra_room;

    	
    	this.g_block_pool = new BlockSimple[g_obj_count];
    	blocks_free_queue = new LinkedBlockingQueue<MemoryStrongReference>(g_obj_count);
		
    	//<block id, block> 
        int how_many_in_cache = Math.max(1, g_obj_count - _extra_room); 
		blocks_in_use = new CacheLRUConcurrent<Long, BlockSimple>(how_many_in_cache, dbfs_prop_instance.concurrent_level);
		cache_capacity = blocks_in_use.getCapacity();
		concurrency_level = blocks_in_use.getConcurrencyLevel();
		
		//thread count depends on the currency level of the memory pool
		//this.thread_executor = Executors.newFixedThreadPool(concurrency_level);
		 	
        this.init(dbfs_prop_instance);
        
        System.out.println("Memory Pool containing " + g_obj_count +" blocks, with each "+_b_size.getSize() +" bytes has been allocated. ");  
    	
    }
  
    private void init(DBFSProperties dbfs_prop_instance)  {  
    	
        for(int k=0;k<g_obj_count;k++)
        {
        	try {
				this.g_block_pool[k] = new BlockSimple(blocks_size, k, dbfs_prop_instance);
			} catch (IndexOutOfBoundsException e) { 
				System.out.println("Exception: failed creating block in memory pool, "
						+ "the initial block size is too small");
				e.printStackTrace();
			}
        }
        
        this.store_size = g_obj_count*((BlockSimple)this.g_block_pool[0]).sizeInBytes();
         
		try {
			this.reset();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		this.shutdown_called = false;
		
    }  
    
    public void reset() throws InterruptedException
    {
    	this.blocks_in_use.clear();
    	this.blocks_free_queue.clear();
    	for(int i =0;i<g_obj_count;i++)
        {
        	//this.blocks_free.add(t_msr);
        	this.blocks_free_queue.put(this.g_block_pool[i].getMemReference());
        }
    }
    
    public CacheLRU getCacheCongruent(int cache_index)
    {
    	return this.blocks_in_use.getCacheSeg(cache_index);
    }
    
    public int getConcurrencyLevel()
    {
    	return this.blocks_in_use.getConcurrencyLevel();
    } 
    
    public int getCapacity()
    {
    	return this.cache_capacity;
    }
    
    private void addToCache(BlockSimple i_block, TManifold outer_storage)  
    {
    	synchronized(this.blocks_in_use.inWhichSeg(i_block.getID())) 
    	{
    		this.blocks_in_use.put(i_block.getID(), i_block);  
    		if(!this.blocks_in_use.inWhichSeg(i_block.getID()).getRemoved().isEmpty())
    		{  
    			/* 
    			 * Since in releasing(writing) a removed block,
    			 * some one else may read it by get(int obj_id, TManifold outer_storage), 
    			 * via cache segment this.blocks_in_use.get(obj_id),
    			 * if gets null, will read from storage. 
    			 * This is unfortunate, the (dirt) block is in the 
    			 * removed queue in writing or waiting to be written.
    			 * Therefore the data is inconsistent.
    			 * The following call for another 
    			 * thread to write is @deprecated    			 * 
    			 */
    			//this.thread_executor.submit(new ObjCollectorInPool(this.blocks_in_use.inWhichSeg(i_block.getID()).getRemoved(), outer_storage, this));  
    			
    			/*
    			 * Instead, we flush the removed block here.
    			 * while this cache segment is locked, 
    			 * no one can get any block via get(id, TManifold),
    			 * just blocking till finish writing and releasing, 
    			 * then release the lock, allow loading blocks including the 
    			 * one just removed and flushed (to storage).   
    			 */
    			flushAndRelease(this.blocks_in_use.inWhichSeg(i_block.getID()).getRemoved(),outer_storage);
    		}
    	}
    } 
    
    private final void flushAndRelease(BlockingQueue<BlockSimple> msr_waiting_for_free, TManifold outer_storage)
    {
    	while(!msr_waiting_for_free.isEmpty())
		{
    		BlockSimple removed = null;
			try
			{
				removed = msr_waiting_for_free.take(); 
				if(!removed.isDirt())
				{ 
					this.tryReleaseMemory(removed.getMemReference(), removed.getID());
					/*
					System.out.println("Thread " + Thread.currentThread().getName() + 
							" released a removed block " 
							+ id_to_remove + ".");
					*/
					continue;
				} 
				
				//long startttt = System.nanoTime(); 
				
				outer_storage.writeObj(removed, removed.getID());
				
				/*
				System.out.println("finished writing a dirt block that Spends: "
		                + (System.nanoTime()-startttt) 
		                + "(ns)");
		                */
				this.tryReleaseMemory(removed.getMemReference(), removed.getID()); 
				/*
				System.out.println("Thread " + Thread.currentThread().getName() + 
						" released a removed dirt block " 
						+ id_to_remove + ".");
						*/
				continue;
			}
			catch(InterruptedException ie)
			{
				Thread.currentThread().interrupt();
			} 
			catch (IOException e) 
			{ 
				System.out.println("Error: failed flushing dirt block to outer storage.");
				e.printStackTrace();
			}  
		}
    }

    public BlockSimple acquirFreeObjMemory() throws InterruptedException
    {
    	if(!shutdown_called)
    	{
    		//MemoryStrongReference t_msr = this.acquirFreeReference();  
    		MemoryStrongReference t_msr = blocks_free_queue.take();
    		
    		if(t_msr == null)
    			return null;  
    		
    		t_msr.occupy();  
    		return this.getMemoryForBlock(t_msr);
    	}
    	throw new IllegalStateException("Block Memory Pool is already shutdown");

    }
    
    public BlockSimple getObj(long obj_id, TManifold outer_storage)  
    {
    	if(obj_id<0)
    		return null;
    	
    	if(!shutdown_called)
    	{
    		BlockSimple t_block = this.blocks_in_use.get(obj_id);
    		if(t_block != null)
    		{ 
    			t_block.getMemReference().increaseMemoryRef();
    			return t_block;
    		}
    		else
    		{ 
    			BlockSimple free_block = null;
    			try {
    				free_block = this.acquirFreeObjMemory();
    				free_block.setID(obj_id); 
    				free_block.read(outer_storage); 
    				free_block.getMemReference().increaseMemoryRef();
    				addToCache(free_block, outer_storage); 
    				return free_block; 
    			}catch (InterruptedException e) {
    				System.err.println("[Error]: can not acquir a free memory to load this block " + obj_id); 
    				e.printStackTrace();
    			}catch (IOException e) {
    				forceReleaseMemory(free_block.getMemReference());
    				System.err.println("[Error]: failed loading block " + obj_id + " from storage.");
    				e.printStackTrace();
    			}
    			 
    			 
    			return null;	
    		}
		}
    	throw new IllegalStateException("Block Memory Pool is already shutdown");
    } 
    
    
    
    private final boolean inPool(MemoryStrongReference msr)
    {
    	if(msr.getIndexInPool()>=0 && msr.getIndexInPool() <this.g_block_pool.length)
    		return true;
    	
    	return false;
    }
    
    public boolean tryReleaseMemory(MemoryStrongReference msr_to_block, long block_id)  
    { 
    	if(msr_to_block==null)
    		return false;
    	if(!inPool(msr_to_block))
    		return false; 
    	 
    	long owner_id = this.g_block_pool[msr_to_block.getIndexInPool()].getID();
    	if(owner_id != block_id)
    	{
    		System.err.println("You are not the owner of this memory block, failed releasing");
    		System.err.println("block to release: " + block_id);
    		System.err.println("but the memory owner block id is: " + owner_id);
    			
    		return false;
    	} 
    	
    	//long startttt = System.nanoTime();  
		
    	synchronized(msr_to_block)
    	{ 
    		//if(!msr_to_block.free())
    		//	return false;
    		msr_to_block.forceFree();
    		this.g_block_pool[msr_to_block.getIndexInPool()].returnToPool();
  
    		try {
				this.blocks_free_queue.put(msr_to_block);
			} catch (InterruptedException e) {
				System.err.println(
						"Error: fail putting released memory reference with index in pool = "
						+ msr_to_block.getIndexInPool() + " to free_block_queue, "
						+ " which means this block memory may never be reused.");
				e.printStackTrace();
				return false;
			}
    		
    		/*
    		System.out.println("finish returning a free block back to queue that Spends: "
                    + (System.nanoTime()-startttt) 
                    + "(ns)");
             */
    		return true;
    	} 
    	   
    }
    
    /*
     * It is unsafe. 
     * the block that has been forceReleased looses its data forever,
     * if it has not write to outer storage in time
     */
    public boolean forceReleaseMemory(MemoryStrongReference msr_to_block)  
    { 
    	if(msr_to_block==null)
    		return false;
    	
    	if(inPool(msr_to_block))
    	{ 
    		synchronized(msr_to_block)
    		{ 
    			msr_to_block.forceFree();
    			this.g_block_pool[msr_to_block.getIndexInPool()].returnToPool();
    			//this.blocks_free.add(msr_to_block);
    			try {
					this.blocks_free_queue.put(msr_to_block);
				} catch (InterruptedException e) {
					System.err.println(
							"Error: fail putting force_released memory reference with index in pool = "
							+ msr_to_block.getIndexInPool() + " to free_block_queue, "
							+ " which means this block memory may never be reused.");
					e.printStackTrace();
					return false;
				}
    			return true;
    		}
    	}  
    	return false;
    }
    
    
    
    protected final BlockSimple getMemoryForBlock(MemoryStrongReference msr)
    {
    	if(!this.shutdown_called)
    	{
    		synchronized(msr)
    		{
    			if(msr==null)
        			return null;
        		if(inPool(msr))
        		{ 
        			msr.increaseMemoryRef();
        			this.g_block_pool[msr.getIndexInPool()].setUnavailable();
        			return this.g_block_pool[msr.getIndexInPool()];
        		}
        		return null;
    		}
    		
    	}
    	throw new IllegalStateException("Block Memory Pool is already shutdown");

    }  
    
    /*
     * returns the block with reference new_block_reference to the cache,
     * and decreases the reference count.
     */
    public void returnObj(MemoryStrongReference new_block_reference, TManifold outer_storage)  
    {
    	if(!this.shutdown_called)
    	{
    		synchronized(new_block_reference)
    		{
    			new_block_reference.decreaseMemoryRef();
    			this.addToCache(this.g_block_pool[new_block_reference.getIndexInPool()], outer_storage);
    		}
    		
    		return;
    	}
    	throw new IllegalStateException("Block Memory Pool is already shutdown");

    }
    
    public int length()
    {
    	return this.g_block_pool.length;
    }
    
    public long size() {
        return this.store_size;
    }  
    
    public BlockSizes getBlockSize()
    {
    	return this.blocks_size;
    }
    
    public int getBlockEntrySize()
    {
    	return this.g_block_pool[0].getEntrySize();
    }
    
    public int blockRecAllowed()
    {
    	return this.g_block_pool[0].recAllowed();
    }
   
    public void shutDown()
    {
    	System.out.println("Memory Pool with block size " + this.blocks_size.getSize() + " bytes is shutting down now......");  
		
    	for(int i = 0; i< this.g_block_pool.length; i++)
    			this.g_block_pool[i] = null;
     
    	this.blocks_in_use.clear(); 
        this.blocks_free_queue.clear(); 
        this.shutdown_called = true;
        //this.thread_executor.shutdown();
        System.out.println("Exited."); 
    } 
   
}

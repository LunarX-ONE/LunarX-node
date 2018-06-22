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
import java.util.concurrent.atomic.AtomicInteger;

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
 * 
 * This lazy allocation version does not pre-allocate all the blocks in memory, 
 * but allocates one when needed, till the cache is full. 
 */

public class BlockCacheLazyAllocate extends BlockCache{
 
   
    protected AtomicInteger g_obj_allocated = new AtomicInteger(0);//maximum is the g_obj_count;
    
    private DBFSProperties g_dbfs_props = null;
    
    
     
    protected BlockCacheLazyAllocate(BlockSizes _b_size, long _blocks_count
    					, DBFSProperties dbfs_prop_instance) { 
    	super(_b_size, _blocks_count , dbfs_prop_instance, false);
     
     
    	g_dbfs_props = dbfs_prop_instance;
    	
    	  
    	this.g_obj_allocated.set(0); 
    	     	
        this.init(dbfs_prop_instance);
        
       
    }
  
    private void init(DBFSProperties dbfs_prop_instance)  {  
    	 
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
    	/*
    	for(int i =0;i<g_obj_count;i++)
        {
        	//this.blocks_free.add(t_msr);
        	this.blocks_free_queue.put(this.g_block_pool[i].getMemReference());
        }
        */
    }
    
 

    public BlockSimple acquirFreeObjMemory() throws InterruptedException
    {
    	if(!shutdown_called)
    	{
    		//MemoryStrongReference t_msr = this.acquirFreeReference();  
    		MemoryStrongReference t_msr = null;
    		if(blocks_free_queue.isEmpty())
    		{
    			if(this.g_obj_allocated.get() < this.g_obj_count )
    			{
    				this.g_block_pool[g_obj_allocated.get()] = new BlockSimple(blocks_size, g_obj_allocated.get(), g_dbfs_props);
    				t_msr = this.g_block_pool[g_obj_allocated.get()].getMemReference(); 
    				g_obj_allocated.incrementAndGet();
    				
    			}
    			else
    				return null;
    		}
    		else
    		{
    			t_msr = blocks_free_queue.take();
        		
        		if(t_msr == null)
        		{ 
        			return null;
        		}
    		}
    		
    			  
    		
    		t_msr.occupy();  
    		return this.getMemoryForBlock(t_msr);
    	}
    	throw new IllegalStateException("Block Memory Pool is already shutdown");

    }
    
    
    
    
    
    private final boolean inPool(MemoryStrongReference msr)
    {
    	//if(msr.getIndexInPool()>=0 && msr.getIndexInPool() <this.g_block_pool.length)
    	if(msr.getIndexInPool()>=0 && msr.getIndexInPool() < this.g_obj_allocated.get())
    		return true;
    	
    	return false;
    } 
   
    public int getBlockEntrySize()
    {
    	if(g_obj_allocated.get()==0)
    	{
    		this.g_block_pool[0] = new BlockSimple(blocks_size, 0, g_dbfs_props);
    		g_obj_allocated.incrementAndGet();
    		try {
				this.blocks_free_queue.put(this.g_block_pool[0].getMemReference());
			} catch (InterruptedException e) {
				 
				e.printStackTrace();
			}
    	}
		
    	return this.g_block_pool[0].getEntrySize();
    }
    
    public int blockRecAllowed()
    {
    	if(g_obj_allocated.get()==0)
    	{
    		this.g_block_pool[0] = new BlockSimple(blocks_size, 0, g_dbfs_props);
    		g_obj_allocated.incrementAndGet();
    		try {
				this.blocks_free_queue.put(this.g_block_pool[0].getMemReference());
			} catch (InterruptedException e) {
				 
				e.printStackTrace();
			}
    	}
		
    	return this.g_block_pool[0].recAllowed();
    }
    
    public void shutDown()
    {
    	System.out.println("Memory Pool with block size " + this.blocks_size.getSize() + " bytes is shutting down now......");  
		
    	for(int i = 0; i< this.g_block_pool.length; i++)
    			this.g_block_pool[i] = null;
     
    	g_obj_allocated.getAndSet(0) ;
    	this.blocks_in_use.clear(); 
        this.blocks_free_queue.clear(); 
        this.shutdown_called = true;
        //this.thread_executor.shutdown();
        System.out.println("Exited."); 
    } 
   
}

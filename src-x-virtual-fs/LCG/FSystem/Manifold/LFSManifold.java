/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG
 * 
 * @author LunarBase team, contact: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License
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
import java.io.IOException;
import java.util.Vector;

import LCG.Concurrent.CacheBig.Controller.Instance.Block4KCache;
import LCG.Concurrent.CacheBig.Controller.impl.BlockCache;
import LCG.Concurrent.CacheBig.Controller.impl.BlockCacheLazyAllocate;
import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.AtomicStructure.EntrySimple;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.StorageEngin.IO.L0.IOInterface;
 
 

/*
 * It is the manager of a group of LFS cardinates  
 * which patches up a whole manifold holding a big table.
 * Be notice: after @appendNewBlock, @getBlock, @getAvailableBlock,
 * MUST call @returnBlock, to return the block back to the cache, 
 * which decreases the reference count so that object collector can 
 * release its memory space it occupies 
 * after it has been eliminated by LRU strategy. 
 */

public class LFSManifold extends TManifold{
 
	public Vector<Long> available_blocks = new Vector<Long>();
	 
	public Vector<Long> damaged_blocks = new Vector<Long>();
		
	private final BlockSizes this_block_size;  
	private final long blocks_in_each_seg;

	public LFSManifold(String _root_path, 
						String _table_name, 
						String _mode, 
						int bufbitlen, 
						BlockSizes b_s, 
						/* BlockCache _pool, */
						BlockCacheLazyAllocate _pool,
						boolean native_mode, 
						DBFSProperties _dbfs_prop_instance) throws IOException 
	{ 
		super(_root_path,_table_name,_mode,bufbitlen, _pool, native_mode, _dbfs_prop_instance);
   
		this_block_size = b_s;
		blocks_in_each_seg = this.seg_file_length/this_block_size.getSize();
		 
		this.max_blocks_in_seg = dbfs_prop_instance.seg_file_size >> b_s.getBitLen();
		super.createCache();
		
		System.out.println("[INFO]: LFS manifold has : " + this.cardinates.size() + " cardinates");  
 
		 
		BlockSimple block_temp = null;
		try {
			block_temp = this._mem_pool.acquirFreeObjMemory(); 
			if(block_temp == null)
			{
				System.err.println("[Error]: the momory pool has been drained before any subsystem starts.");
				System.err.println("[Error]: please check the OS you use and restart LunarBase......");
				return;
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		IOInterface ios_t = (IOInterface)this.cardinates.get(0);
		if(ios_t.length() == 0 || object_count == 0)
		{
			System.out.println("[INFO]: No data has been added yet");
			object_count = 0;
			return;			
		}
		ios_t.seek(0);
		block_temp.ReadEntry(ios_t);
		if(block_temp.sizeInBytes() != this_block_size.getSize())
		{
			System.err.println("[Error]: please use the correct class to read this cardinate");
			System.err.println("[Error]: The real block size is: " + block_temp.sizeInBytes());  
			//throw new IOException("Loading a file with wrong block size parameter: " + this_block_size.getSize());				
		}
		else
		{
			block_temp.ReadData(ios_t); 
		}
		
		this._mem_pool.returnObj(block_temp.getMemReference(), this);
		
		System.out.println("This manifold has objects : " + object_count);  

		long start = Math.max(0, object_count - this._mem_pool.getCapacity());
		System.out.println("Loading the latest: " + this._mem_pool.getCapacity() + " blocks");  
		 
		/*
		 * It's time consuming for reading all blocks through
		 * when loading the manifold
		 */
		for(long i=start;i < object_count; i++)
		{ 
			BlockSimple block_i = null;
			block_i = this._mem_pool.getObj(i, this);  
		} 
		
		//check every block, and put the available block to list for use
		//checkAvalabilityThroughAll();
		checkAvailabilityViaDir();
	} 
	
	private void checkAvailabilityViaDir() throws IOException
	{
		synchronized(this.objects_dir)
		{
		if(this.object_count == 0)
			return;
			
		objects_dir.seek(0);
		byte[] block_availablity_in_seg = new byte[(int)blocks_in_each_seg];
		for(int k=0;k<this.interfaceCount();k++)
		{  
			int iter = (int)blocks_in_each_seg;
			if(k == this.interfaceCount()-1)
			{
				//long blocks_in_latest = blocks_in_each_seg-(k*blocks_in_each_seg - this.object_count);
				long blocks_in_latest = this.object_count  - k*blocks_in_each_seg;
				
				block_availablity_in_seg = new byte[(int)blocks_in_latest];
				iter = (int)blocks_in_latest; 
			} 
			this.objects_dir.read(block_availablity_in_seg);
			
			for(int i = 0; i< iter;i++)
			{
				
				if(DBFSProperties.getAvailability((int)block_availablity_in_seg[i]).equals(DBFSProperties.available))
				{
					available_blocks.add(blocks_in_each_seg*k+i);
				}
				else if(DBFSProperties.getAvailability((int)block_availablity_in_seg[i]).equals(DBFSProperties.unavailable))
				{
					//
				}
				else
				{
					byte bbb = DBFSProperties.getAvailability((int)block_availablity_in_seg[i]);
					damaged_blocks.add(blocks_in_each_seg*k+i);
				} 
			}
		}
		}
		
	} 
	 
	public boolean validity(EntrySimple e_s)
	{
		if(e_s.getAvailability().equals(DBFSProperties.unavailable) 
	    			|| e_s.getAvailability().equals(DBFSProperties.available))
			return true;
	    	
		return false; 
	}
	
	protected long calcDataLengthInLatestIOStream()
	{
		 
		int interface_count = Math.max(1,this.cardinates.size()); 
		 
		long blocks_in_latest = blocks_in_each_seg-(interface_count*blocks_in_each_seg - this.object_count);
		return blocks_in_latest * this_block_size.getSize();
	}
	
	public BlockSimple appendNewBlock()  
	{  
		BlockSimple new_block = null;
		try {
			new_block = this._mem_pool.acquirFreeObjMemory(); 
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		}		
		if(new_block == null) 
			return null; 
		
		long id = appendNewInDisk(new_block);
		if(id >= 0)
		{ 
			/*
			 * since acquired above, then return it to 
			 * keep the reference count correct
			 */
			this._mem_pool.returnObj(new_block.getMemReference(), this);
			/*
			 * again get it from pool;
			 */
			return this._mem_pool.getObj(id, this);
			 
		}
		else
		{
			if(!new_block.getMemReference().getStatus())
				this._mem_pool.forceReleaseMemory(new_block.getMemReference());
				 
			  
			return null;
		}
	}  
	
	public int getBlockEntrySize()
	{
		return this._mem_pool.getBlockEntrySize();
	}
	
	public int getBlockSize()
	{
		return this.this_block_size.getSize();
	}
	
	public int blockRecAllowed()
	{
		return this._mem_pool.blockRecAllowed();
	}
	
	public final long queryBlockPosition(int block_id)
	{
		return (long)(block_id * (long)this.this_block_size.getSize());
	}
	
	public BlockSimple getBlock(long block_id) 
	{  
		//return (BlockSimple) super.getObj(block_id); 
		return this._mem_pool.getObj(block_id, this);
	}
	
	/*
	 * remove data, and set available
	 */
	public void setAvailable(BlockSimple _block) throws IOException 
	{ 
		/*
		 * Must set the block to be available, in which process 
		 * cleans the data in it for accommodating new data. 
		 */
		_block.setAvailable(); 
		synchronized(this.available_blocks)
		{
			synchronized(this.objects_dir)
			{
				this.objects_dir.seek(_block.getID()); 
				this.objects_dir.write(DBFSProperties.available);
				 
				/*
				 * Here needs no flushing immediately, since even if the system crashes, 
				 * the block is still marked unavailable, 
				 * and when the system reboots, this block will not be added to 
				 * the available block list, and never be visited again. 
				 * 
				 * It will leave garbage on the disk, but is not a big deal 
				 * comparing the time it saves.
				 *  
				 */
				//this.objects_dir.flushSync();
			}
			
			this.available_blocks.add(_block.getID()); 
		} 
	} 
	
	/*
	 * remove data, and set it available
	 */
	public void setAvailable(long _block_id) throws IOException 
	{ 
		setAvailable(this._mem_pool.getObj(_block_id, this)); 
	} 
	
	
	/*
	 * return to pool, otherwise, it can not be released automatically,
	 * i.e the reference count never decrease to 0.
	 */
	public void returnBlock(BlockSimple _block)  
	{
		this._mem_pool.returnObj(_block.getMemReference(), this); 
	}
	
	public BlockSimple getAvailableBlock() 
	{
		synchronized(this.available_blocks)
		{
			while(this.available_blocks.size()>0)
			{
				int s = this.available_blocks.size();
				long id = this.available_blocks.remove(s-1);
				 
				BlockSimple _b_s = (BlockSimple) this.getBlock(id);
				if(_b_s == null)
					/*
					 * failed, try the next. 
					 * And it has no need to return this null block   
					 */
					continue;
				
				_b_s.setUnavailable(); 
				try {
					this.objects_dir.seek(id);
					this.objects_dir.write(DBFSProperties.unavailable);
					/*
					 * here flushes immediately, in case system failure occurs,
					 * then blocks unavailable will still be available. 
					 * Then when system reboot, the data in 
					 * it may be damaged. 
					 */
					this.objects_dir.flushSync();
				} catch (IOException e) {
					returnBlock(_b_s);
					e.printStackTrace();
					continue;
				} 
				
				return _b_s;  
			}  
		} 
		
		return null;
	}
	
	public int getMyLevel()
	{
		return this.this_block_size.getLevel();
	}
	
	public IOInterface getDir()
	{
		synchronized(this.objects_dir)
		{
			return this.objects_dir;
		}
	}
	
	public Vector<Long> getAvailableAll()
	{
		synchronized(this.available_blocks)
		{
			return this.available_blocks;
		}
	}
	
	public void close() throws IOException
	{ 
		available_blocks = null; 
		damaged_blocks = null; 
		super.close();
	}
 
	 
}

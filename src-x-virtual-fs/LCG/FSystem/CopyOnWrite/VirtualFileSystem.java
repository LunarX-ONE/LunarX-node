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
 


package LCG.FSystem.CopyOnWrite;

import java.io.IOException;
import java.util.Vector;

import LCG.Concurrent.CacheBig.Controller.Instance.Block4KCache;
import LCG.Concurrent.CacheBig.Controller.Instance.Block512BCache;
import LCG.Concurrent.CacheBig.Controller.Instance.Block64BCache;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes; 
import LCG.FSystem.Instance.LFS512B; 
import LCG.FSystem.Instance.LFSFactory;
import LCG.FSystem.Manifold.LFSManifold;

public class VirtualFileSystem {  
	
	final DBFSProperties dbfs_prop_instance;
	final int _max_level_count ;
	final LFSManifold[] _base_manifolds;
	final int[] _jump_tolerants;
	 
	
	public VirtualFileSystem(String _root_path, 
								String _column_name, 
								String _mode, 
								int _bufbitlen, 
								boolean _native_mode, 
								DBFSProperties _dbfs_prop_instance) throws IOException {
		 
		dbfs_prop_instance = _dbfs_prop_instance;
		_max_level_count = Math.max(_dbfs_prop_instance.max_level_count, 2);
		 
		_base_manifolds = new LFSManifold[_max_level_count ]; 
		
		/*
		_base_manifolds[0] = new LFS64B(_root_path, _table_name,_mode, _bufbitlen, _native_mode);
		_base_manifolds[1] = new LFS512B(_root_path, _table_name,_mode, _bufbitlen, _native_mode);
		_base_manifolds[2] = new LFS4K(_root_path, _table_name,_mode, _bufbitlen, _native_mode);
	
		*/
		
		LFSFactory lfs_factory = new LFSFactory(_root_path, 
										_column_name, 
										_mode,
										_bufbitlen,
										_native_mode,
										dbfs_prop_instance);
		for(int i=0; i < _max_level_count; i++)
		{
			_base_manifolds[i] = lfs_factory.generateManifoldPatch(i);
		}
		_jump_tolerants = new int[_max_level_count];
		calculateJumpTolarant();
		
	}
	
	private void calculateJumpTolarant()
	{
		for(int i=0;i < _base_manifolds.length-1;i++)
		{
			int level_rec_allowed = _base_manifolds[i].blockRecAllowed();
			int upper_rec_allowed = _base_manifolds[i+1].blockRecAllowed();
			_jump_tolerants[i] = (int) upper_rec_allowed/level_rec_allowed;
		} 
		_jump_tolerants[_base_manifolds.length-1] = Integer.MAX_VALUE;
	}
	
	public int levelTolerant(int level)
	{
		if(inRange(level))
			return _jump_tolerants[level];
		return -1;
	}
	
	public int levelBlockRecAllowed(int level)
	{
		if(inRange(level))
			return _base_manifolds[level].blockRecAllowed();
			
		return -1;
	}
	
	public LFSManifold getLevelManifold(int level)
	{
		if(inRange(level))
			return _base_manifolds[level];
		else
			return null;
	}
	
	private boolean inRange(int level)
	{
		return (level >= 0 && level <  _max_level_count);
	}
	public LFSManifold getLevelManifold(BlockSimple b_s)
	{
		if(inRange(b_s.getMyLevel()))
			return _base_manifolds[b_s.getMyLevel()];
		return null;
	} 
	 
	
	public BlockSimple getVFileBlock(VFileHandler _f_h, BlockSimple point_to) throws IOException
	//public BlockSimple getVFileBlock(VFileHandler _f_h) throws IOException
	{
		/*
		if(inRange(_f_h.handler_level))
			return this.getLevelManifold(_f_h.handler_level).getBlock(_f_h.handler_id);
		return null;
		*
		*/
		return getVFileBlock(_f_h.handler_level, _f_h.handler_id, point_to);
		//return getVFileBlock(_f_h.handler_level, _f_h.handler_id);
	}
	
	public BlockSimple getVFileBlock(int level, long block_id, BlockSimple point_to) throws IOException
	//public BlockSimple getVFileBlock(int level, int block_id) throws IOException
	{	/*
		 * if get block with block_id point to an existing block,
		 * first decrease the memory reference of the original 
		 * block memory.
		 */
		if(point_to!=null)
			point_to.getMemReference().decreaseMemoryRef();
		
		if(inRange(level) && block_id>=0)
		{
			return this.getLevelManifold(level).getBlock(block_id);
		}
		 
		return null;
		 
		
	}
	
	public void writeVFileBlock(BlockSimple _b_s) throws IOException
	{
		this.getLevelManifold(_b_s.getMyLevel()).writeObj(_b_s, _b_s.getID());
	}
	
	//public int levelOnDemand(int _bytes_needed)
	public int levelOnDemand(int _rec_counts)
	{
		/*
		 * seek from the maximum level.
		 * Always try the biggest blocks for trivial reason.
		 */
 
		int start_level = _max_level_count-1 ; 
		//BlockSizes b_s = BlockFlages.getBlockSize(start_level);
		
		//while((_bytes_needed / (b_s.getSize() - this.getLevelManifold(start_level).getBlockEntrySize() ))==0)
		while((_rec_counts / this.getLevelManifold(start_level).blockRecAllowed()) == 0)
		{
			start_level--;
			if(start_level<0)
			{ 
				start_level=0;
				break;
			}
			//b_s = BlockFlages.getBlockSize(start_level);
		}
		
		return start_level;
		 
		
	}
	
	public BlockSimple acquirAvailableBlock(int level, BlockSimple point_to)  
	{
		/*
		 * if block with block_id points to an existing block,
		 * first decrease the memory reference of the original 
		 * block memory.
		 */
		if(point_to!=null)
			point_to.getMemReference().decreaseMemoryRef();
		
		point_to = this.getLevelManifold(level).getAvailableBlock();
		if(point_to == null)
			point_to = this.getLevelManifold(level).appendNewBlock(); 
		
		return point_to;
	}
	
	public boolean blockGarbo(Vector<Long> _block_garbage)
	{
		int count = 0;
		for(int i =0; i<_block_garbage.size();i+=2)
		{
			int _level = (int)(_block_garbage.elementAt(i).longValue());
			long _id = _block_garbage.elementAt(i+1);
			try
			{
				this.getLevelManifold(_level).setAvailable(_id);
			}
			catch(IOException e)
			{
				System.err.println("[Error]: failed setting block "
						+ _id + " at level "
						+ _level 
						+ " to be available @VirtualFileSystem.blockGarbo(Vector<Long> _block_garbage). "
						+ "This leaves garbage on disk");
				count++;
				continue;
			}			
		}
		
		return (count==0?true:false);
	}
	
	public boolean blockGarbo(long _block_garbage, int _level)
	{
		try
		{
			this.getLevelManifold(_level).setAvailable(_block_garbage);
		}
		catch(IOException e)
		{
			System.err.println("[Error]: failed setting block "
					+ _block_garbage + " at level "
					+ _level 
					+ " to be available @VirtualFileSystem.blockGarbo((long _block_garbage, int _level)). "
					+ "This generates garbage on disk");
			return false;
		}			
		 
		return true;
	}
	 
	
	public int levelCount()
	{
		return this._max_level_count-1;
	}
	public void flushCache() 
	{
		for(int i=0;i<_base_manifolds.length;i++)
			try {
				_base_manifolds[i].flushCache();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public void flushBaseIO() 
	{
		for(int i=0;i<_base_manifolds.length;i++)
			try {
				_base_manifolds[i].flush() ;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	public void close()
	{ 
		for(int i=0;i<_base_manifolds.length;i++)
			try {
				_base_manifolds[i].shutdownPool();
				_base_manifolds[i].close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	

}

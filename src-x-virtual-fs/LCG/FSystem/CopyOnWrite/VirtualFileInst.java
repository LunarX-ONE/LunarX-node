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

import LCG.FSystem.AtomicStructure.BlockSimple; 
import LCG.FSystem.Manifold.LFSManifold;
import LCG.FSystem.Manifold.SubManifoldInterface; 

/*
 * level i: -1 <-- |__| <-- |__| <-- ... <-- |__|
 * 		      ---------------------------------^
 * level i+1: ^--|__| <-- |__| <-- ... <-- |__|
 * ...        -------------------------------^
 * max_level: ^--|__| <-- |__| <-- ... <-- |__|
 * 
 * descriptor:
 * level i: [latest id][count on this level][oldest id]
 */
public class VirtualFileInst implements SubManifoldInterface{
	 	 
	//handler is the latest block on this submanifold,
	//and it must lay on the lowest level of this submanifold. 
	final VFileHandler f_handler = new VFileHandler(); 
 	 
	//<level, manifold>
	final VirtualFileSystem _vfs;
	final int _max_level;
	/* 
	 * level 0: latest block i, block count on this level, oldest block j
	 * level 1: latest block i, block count on this level, oldest block j
	 * ...
	 * level _max_level: latest block i, block count on this level, oldest block j
	 * 
	 */
	//
	public final static int descriptor_dim = 3;
	private final int latest_indicator = 0;
	private final int count_indicator = 1;
	private final int oldest_indicator = 2;
	final long[][] _vf_descriptor;
	
	
	private LFSManifold _current_level_manifold;
	/*
	 * garbage blocks. This vector keeps pairs:
	 * (level1, id1),(level2, id2)...
	 */
	private Vector<Long> block_garbage;
	
	//create = true: create a new submanifold, whose handler id is allocated by manifold system.
	//create = false: read an existing submanifold from file system with the beginning id
	 
	/*
	 * Constructor on create mode
	 */
	public VirtualFileInst(VirtualFileSystem _v_f) throws IOException 
	{
		this._vfs = _v_f;
		this.f_handler.handler_id = 0;
		this.f_handler.handler_level = 0;
		_current_level_manifold = _vfs.getLevelManifold(this.f_handler.handler_level);
		_max_level = _vfs.levelCount();
		_vf_descriptor = new long[_max_level+1][descriptor_dim];
		for(int i=0;i<_max_level+1;i++)
		{
			_vf_descriptor[i][latest_indicator] = -1;
			_vf_descriptor[i][count_indicator] = 0;
			_vf_descriptor[i][oldest_indicator] = -1;
		} 
		
		block_garbage = new Vector<Long>();
		
		 
			//if create, always starts from the lowest level,
			_current_level_manifold = _vfs.getLevelManifold(0);
			//and the handler is a new block in this level.
			BlockSimple _handler = _current_level_manifold.appendNewBlock();  
			validateHandler(_handler);
			f_handler.handler_id = _handler.getID();
			f_handler.handler_level = _handler.getMyLevel();
			_vf_descriptor[f_handler.handler_level][latest_indicator] = f_handler.handler_id;
			_vf_descriptor[f_handler.handler_level][count_indicator] = 1;
			_vf_descriptor[f_handler.handler_level][oldest_indicator] = f_handler.handler_id; 
		 
	}
	
	public VirtualFileInst(VirtualFileSystem _v_f, long _handler_id, int _handler_level) throws IOException
	{
		this._vfs = _v_f;
		this.f_handler.handler_id = _handler_id;
		this.f_handler.handler_level = _handler_level;
		_current_level_manifold = _vfs.getLevelManifold(this.f_handler.handler_level);
		_max_level = _vfs.levelCount();
		_vf_descriptor = new long[_max_level+1][3];
		for(int i=0;i<_max_level+1;i++)
		{
			_vf_descriptor[i][latest_indicator] = -1;
			_vf_descriptor[i][count_indicator] = 0;
			_vf_descriptor[i][oldest_indicator] = -1;
		} 
		
		block_garbage = new Vector<Long>();
		
		 
		 
			//BlockSimple _handler  = _current_level_manifold.getBlock(f_handler.handler_id);  
			BlockSimple _handler  = null;
			_handler = this._vfs.getVFileBlock(f_handler, null); 
			validateHandler(_handler); 
	    	
	    	//Load all the blocks below the max level.
	    	//E.G. if the max level is 2, then load the blocks in level 0 and 1.
	    	int current_level = _handler.getMyLevel();
	    	_vf_descriptor[current_level][latest_indicator] = _handler.getID();
	    	_vf_descriptor[current_level][count_indicator] = 1;
	    	BlockSimple _i_block = _handler;
	    	/*
	    	 * load all the blocks that under the max level.
	    	 * In linux language, these blocks are tails, waiting to be merged
	    	 * into a bigger block.
	    	 * 
	    	 * They are not many, since if they reach the size of 
	    	 * a block 4K(for example), they will be merged into a new 
	    	 * 4K block, these blocks holding tail data are cleared 
	    	 * and set to be available.  
	    	 * 
	    	 */ 
	    	 
	    	while(current_level < this._max_level)
	    	{ 
	    		long _next_id = _i_block.getNext();
	    		int next_level = _i_block.getNextLevel(); 
	    		
	    		if(current_level == next_level)
	    		{ 
	    			//_i_block = this._vfs.getVFileBlock(current_level,_next_id);
	    			_i_block = this._vfs.getVFileBlock(current_level,_next_id, _i_block);
	    			_vf_descriptor[current_level][count_indicator]++;
	    		}
	    		else
	    		{
	    			_vf_descriptor[current_level][oldest_indicator] = _i_block.getID();
	    			if(next_level >= this._max_level)
	    			{
	    				/*
	    				 * for the max level, only set the latest 
	    				 * block id to _vf_descriptor, for the 
	    				 * purpose of saving loading time 
	    				 */
	    				current_level = next_level; 
		    			//_i_block = this._vfs.getVFileBlock(current_level,_next_id);
	    				_i_block = this._vfs.getVFileBlock(current_level,_next_id, _i_block);
		    			_vf_descriptor[current_level][latest_indicator] = _i_block.getID();
		    			_vf_descriptor[current_level][count_indicator]++; 
	    				
	    				break;
	    			}
	    			if(_next_id == -1 || next_level == -1)
		    			break;
		    			
	    			current_level = next_level; 
	    			//_i_block = this._vfs.getVFileBlock(current_level,_next_id);
	    			_i_block = this._vfs.getVFileBlock(current_level,_next_id,_i_block);
	    			_vf_descriptor[current_level][latest_indicator] = _i_block.getID();
	    			_vf_descriptor[current_level][count_indicator]++; 
	    		} 
	    	}
		 
	} 
	
	
	public VirtualFileInst(VirtualFileSystem _v_f, long[][] vf_discriptor )
	{
		this._vfs = _v_f;
		this._max_level = _vfs.levelCount();
		
		this._vf_descriptor = vf_discriptor;
	}
	
	public boolean loadHanlder( long _handler_id,  int _handler_level ) throws IOException  
	{
	 
		this.f_handler.handler_id = _handler_id;
		this.f_handler.handler_level = _handler_level;
		_current_level_manifold = _vfs.getLevelManifold(this.f_handler.handler_level);
		 
		
		 
		block_garbage = new Vector<Long>();
		
		 
		 
		BlockSimple _handler  = null;
		_handler = this._vfs.getVFileBlock(f_handler, null); 
		if(!validateHandler(_handler))
			return false;
		
		return true;
		 /*
			try {
				validateHandler(_handler);
			} catch (IOException e) {
				 
				e.printStackTrace();
				System.out.println("get " + key + " ,,,,,,,,,,,,,"); 
					
			}  
		 */
		 
		
	    	 
	} 
	
	private final boolean validateHandler(BlockSimple _handler) 
	{
		if(_handler == null)
		{	//throw new IOException("Error: chain handler is NULL, it does not exist, or was damaged, can not get any infomation of this chain.");
			System.err.println("[Error]: @VirtualFileInst.validateHandler(...), chain handler is NULL, it does not exist, or was damaged, can not get any infomation of this chain.");
			return false;
		}
		if(_handler.recElementBytes() != Integer.BYTES)
		{
			//throw new IOException("Error: use the wrong virtual file system to store data. Data element must be a 4 bytes integer");
			System.err.println("Error: use the wrong virtual file system to store data. Data element must be a 4 bytes integer");
			return false;
		} 
    	if(!_current_level_manifold.validity(_handler.getEntry()))
    	{
    		//throw new IOException("[Error]: chain handler with this block id: " 
			//	+ _handler.getID() +" may be damaged, can not validate it"); 
    		/*
    		 * how possible the handler has its availabibity to be -1? no available nor unavailable
    		 */
    		System.err.println("[Error]: chain handler with this block id: " 
    				 	+ _handler.getID() +" may be damaged, can not validate it"); 
			return false;
    	}
    	
    	if(_handler.getPrev() != -1)
    	{
    		//throw new IOException("Error: the block with id: " 
			//		+ _handler.getID() + " is not a virtual file handler"); 
    		/*
    		 * This situation may occur when appending data to an old handler, 
    		 * and generates a new one, the old handler points to the new one, 
    		 * hence getPrev()!=-1, and saved. 
    		 * 
    		 * But at this moment, the system unfortunately crashes, 
    		 * new handler information and descriptor are lost. 
    		 * 
    		 * Then when lunarbase restarts, it still seeks this old handler for data. 
    		 * 
    		 * Within absorbTo( int target_level), and after @Step 3, 
    		 * the prev block is not -1 any more, but it is still the system handler.
    		 * 
    		 * Do nothing, just set the prev -1, and append new data to this old handler. 
    		 * What were lost are lost.
    		 */
    		_handler.setPrev(-1);
    		_handler.setPrevLevel(-1);
    		System.err.println("[Error]: the block with id: " 
    					+ _handler.getID() + " is not a virtual file handler"); 
    		return false;
    	}
    	
    	return true;
	}
	
	public static long[][] createVFDiscriptor(int level_count)
	{
		long[][] descriptor = new long[level_count][descriptor_dim];
		for(int i=0;i<level_count;i++)
		{
			descriptor[i][0] = -1;
			descriptor[i][1] = 0;
			descriptor[i][2] = -1;
		} 
		
		return descriptor;
	}
	public void printData() throws IOException
	{ 
		//BlockSimple _latest_file_block = null;
		BlockSimple _latest_file_block = this._vfs.getVFileBlock(f_handler, null);
		while(_latest_file_block != null)
		{ 
			for(int i=_latest_file_block.getRecCount()-1;i>=0;i--)
			{
				System.out.println("Level: "+ _latest_file_block.getMyLevel() 
									+ ", Block " + _latest_file_block.getID() 
									+ ", has record " + _latest_file_block.getRecordAt(i));
			} 
			int next_level = _latest_file_block.getNextLevel(); 
			//_latest_file_block = this._vfs.getVFileBlock(next_level, _latest_file_block.getNext()); 
			_latest_file_block = this._vfs.getVFileBlock(next_level, _latest_file_block.getNext(), _latest_file_block); 
		}
	}
	
	/*
	 * read data from this virtual file.
	 * If _filter is NULL, returns all the data it contains.
	 * the length of data_result is the maximum number of records 
	 * that should be returned. 
	 * If total data count exceeds data_result.lenght, 
	 * take the latest data_result.lenght records.
	 * 
	 * Be noticed: readData copies block data to a user defined 
	 * data buffer data_result, this causes a lower performance. 
	 * A high efficient alternative is @readDataMemoryMap, 
	 * which locks the blocks in heap memory(actually in 
	 * BigCache that Lunar virtual file system implements),  
	 * and user is permitted to manipulate the data directly. 
	 * 
	 * data in data_result is from oledest to the latest:
	 * oldest        ------------------> latest
	 * data_result[0]------------------> data_result[my_size]
	 */
	public void readData(int[] data_result) throws IOException
	{ 
		//BlockSimple _begin_block = null;
		BlockSimple _begin_block = this._vfs.getVFileBlock(f_handler, null);
	 
		int indicator = data_result.length;
		while(_begin_block != null)
		{ 
			int allowed = (_begin_block.getRecCount() <= indicator)?
							_begin_block.getRecCount()
							:indicator;

			System.arraycopy(_begin_block.getData(), _begin_block.getRecCount()-allowed, data_result, indicator-allowed, allowed);
			indicator = indicator - allowed;	
			if(indicator <= 0)
			{
				_begin_block.getMemReference().decreaseMemoryRef();
				return;
			}
			int next_level = _begin_block.getNextLevel(); 
			//_begin_block = this._vfs.getVFileBlock(next_level, _begin_block.getNext()); 
			_begin_block = this._vfs.getVFileBlock(next_level, _begin_block.getNext(), _begin_block); 
		} 
		//_begin_block.getMemReference().decreaseMemoryRef();
	}
	
	public void readData(int[] data_result, int __from, int __len) throws IOException
	{ 
		int len = __len;
		if(__from+ __len > data_result.length)
			len =  data_result.length - __from;
			
		//BlockSimple _begin_block = null;
		BlockSimple _begin_block = this._vfs.getVFileBlock(f_handler, null);
	 
		int indicator = __from + len;
		while(_begin_block != null)
		{ 
			int allowed = (_begin_block.getRecCount() <= indicator)?
							_begin_block.getRecCount()
							:indicator;

			System.arraycopy(_begin_block.getData(), _begin_block.getRecCount()-allowed, data_result, indicator-allowed, allowed);
			indicator = indicator - allowed;	
			if(indicator <= __from)
			{
				_begin_block.getMemReference().decreaseMemoryRef();
				return;
			}
			int next_level = _begin_block.getNextLevel(); 
			//_begin_block = this._vfs.getVFileBlock(next_level, _begin_block.getNext()); 
			_begin_block = this._vfs.getVFileBlock(next_level, _begin_block.getNext(), _begin_block); 
		} 
		_begin_block.getMemReference().decreaseMemoryRef();
	}
	
	public void readDataMemoryMap()
	{
		/*
		 * this is the non-copy version of @readData,
		 * but seems impossible?
		 */
	}
	
	public long[][] getVFDiscriptor()
	{
		return this._vf_descriptor;
	}
	/*
	 * must call before @updateDescriptorAfterAppending. 
	 * A correct linkage guarantees the correct merge process 
	 * by @absorb 
	 */
	private final void linkAToB(BlockSimple a, BlockSimple b)
	{
		a.setNext(b.getID());
		a.setNextLevel(b.getMyLevel()); 
		b.setPrev(a.getID());
		b.setPrevLevel(a.getMyLevel());
	}
	
	/*
	 * After appending a new block to this virtual file, 
	 * its descriptor must update. This function may 
	 * trigger the merging action if the blocks on level new_block.getMyLevel()
	 * exceed the level tolerant threshold.
	 * 
	 * It is called after @linkAToB, which links the new block to 
	 * the original latest block b. If fails to link, the absorb 
	 * function will fail and the data will lose.
	 */
	private final BlockSimple updateDescriptorAfterAppending(BlockSimple new_block) throws IOException
	{
		this._vf_descriptor[new_block.getMyLevel()][latest_indicator] = new_block.getID();
		this._vf_descriptor[new_block.getMyLevel()][count_indicator]++; 
			
		if(this._vf_descriptor[new_block.getMyLevel()][count_indicator]==1
				&& new_block.getMyLevel() < _max_level)
		{ 
			this._vf_descriptor[new_block.getMyLevel()][oldest_indicator] = new_block.getID(); 
		}
		if(this._vf_descriptor[new_block.getMyLevel()][count_indicator] >= _vfs.levelTolerant(new_block.getMyLevel())
				&& new_block.getMyLevel() < _max_level)
		{
			new_block.getMemReference().decreaseMemoryRef();
			return absorbTo(new_block.getMyLevel()+1);
		}
		else
		{ 
			return new_block; 
		} 
	}
	/*
	 * returns the position of the last record that is successfully appended
	 */
	public int appendData(int[] records, int from, int length) throws IOException 
	{ 
		int i_start = from;
		int end = Math.min(records.length-1, from+length-1);
		BlockSimple _handler_block = null;
		_handler_block = this._vfs.getVFileBlock(this.f_handler, _handler_block);
		
		while(i_start<=end)
		{ 
			/*
			 * first fill up the handler, 
			 * it's the latest block in this submanifold 
			 */ 
			synchronized(_handler_block)
			{
				int i_end = _handler_block.appendRecords(records, i_start, end-i_start+1);
				 
				i_start = i_end+1; 
			 
				if(i_start > end) 
					break; 
			
				//int bytes_needed = (end - i_start +1) * Integer.BYTES;
				int rec_needed = (end - i_start +1);
				 
				int level_needed = 0; 
				level_needed = _vfs.levelOnDemand(rec_needed); 
				BlockSimple b_s_new = null;
				if(level_needed > _handler_block.getMyLevel())
				{
					/* 
					 * If level_needed is bigger than handler block's 
					 * level, then this requires a jump from lower level 
					 * to this higher one.
					 * First of all, check if blocks on this level_needed 
					 * exceeds the level tolerant count. 
					 * If it is, merge the blocks lay in level needed 
					 * to a higher level first.
					 * 
					 * Then, 
					 * copy all the latest data to this level,
					 * which creates a new block in this level
					 */
					b_s_new = absorbTo(level_needed);  
					
					//int blocks_count_at_target_level = this._vf_descriptor[level_needed][count_indicator];
					
					//if(blocks_count_at_target_level >= _vfs.levelTolerant(level_needed)
					//		&&  level_needed < _max_level)
					//{
					//	b_s_new = absorbTo(level_needed+1);  
					//}
					 
					
					/*
					 * After being absorbed, the handler block data 
					 * and all the blocks which are under the level of 
					 * level_needed have their data copied into  
					 * b_s_new. Therefore it becomes the latest block 
					 * of this virtual file, which means it has 
					 * no previous block, as well as a previous level. 
					 * Just evaluating the file handler to be b_s_new is 
					 * enough.
					 *  
					 */
					_handler_block.getMemReference().decreaseMemoryRef();
					_handler_block = b_s_new; 
				}  
				else
				{
					/*
					 * If level_needed equals to, or is smaller than, 
					 * the handler block's level, no jump happens.
					 * 
					 * But must check if the total blocks count is 
					 * bigger than the level tolerant count, 
					 * after this new block add to its 
					 * level.
					 * 
					 * If it is, merge the blocks in level_needed 
					 * to its upper level 
					 *  
					 */
					//int blocks_count_after_add_one = this._vf_descriptor[level_needed][count_indicator] + 1;
					
					//if(level_needed == _handler_block.getMyLevel() 
					//		&& blocks_count_after_add_one > _vfs.levelTolerant(level_needed)
					//		&&  level_needed < _max_level)
					//{
					//	b_s_new = absorbTo(level_needed+1); 
					//	_handler_block = b_s_new; 
					//} 
					//else
					//{
						//b_s_new = _vfs.acquirAvailableBlock(level_needed); 
						b_s_new = _vfs.acquirAvailableBlock(level_needed, b_s_new); 
						if(b_s_new == null)
						{
							System.out.println("Error: appending a new block failed for unknown reason.......");
							System.out.println((end - i_start + 1) 
										+ " records fail adding to a new block.");
							//return _handler_block;
							return i_end;
						}
					 
						/*
						 * @Step1: link to handler block
						 */
						linkAToB(b_s_new, _handler_block);  
						/*
						 * @Step2: update the descriptor, merge if threashold 
						 * condition satisfied
						 */
						_handler_block.getMemReference().decreaseMemoryRef();
						_handler_block = updateDescriptorAfterAppending(b_s_new);
					//}
				} 
				this._current_level_manifold = _vfs.getLevelManifold(_handler_block);
				  
			} 
		}
		this.f_handler.handler_id = _handler_block.getID();
		this.f_handler.handler_level = _handler_block.getMyLevel();
 
		//return _handler_block;
		return end;
	}
	
	/*
	 * Differ from the appendData(...), which always seeks the 
	 * blocks in the possible lowest level, 
	 * this greedy version appends data on the handler level, 
	 * creating new block on this level if needed. 
	 * 
	 * It is assumed that if one term has, say 1000 documents including it,
	 * it will have more 1000 documents in the future. 
	 * Since all terms are uniformly distributed .
	 * 
	 * This greedy version may waste some block space, but greatly 
	 * decreases the times of block merging from lower level to 
	 * a higher one.
	 *  
	 */
	public int appendDataGreedy(int[] records, int from, int length) throws IOException 
	{ 
		int i_start = from;
		int end = Math.min(records.length-1, from+length-1);
		BlockSimple _handler_block = null;
		_handler_block = this._vfs.getVFileBlock(this.f_handler, _handler_block);
		
		while(i_start<=end)
		{ 
			/*
			 * first fill up the handler, 
			 * it's the latest block in this submanifold.
			 */ 
			synchronized(_handler_block)
			{
				int i_end = _handler_block.appendRecords(records, i_start, end-i_start+1);
				this._vfs.writeVFileBlock(_handler_block); 
				
				i_start = i_end+1; 
			 
				if(i_start > end) 
					break; 
			
				//int bytes_needed = (end - i_start +1) * Integer.BYTES;
				int rec_needed = (end - i_start +1);
				 
				int level_needed = 0; 
				level_needed = _vfs.levelOnDemand(rec_needed); 
				BlockSimple b_s_new = null;
				if(level_needed > _handler_block.getMyLevel())
				{
					/* 
					 * If level_needed is bigger than handler block's 
					 * level, then this requires a jump from lower level 
					 * to this higher one.
					 * First of all, check if blocks on this level_needed 
					 * exceeds the level tolerant count. 
					 * If it is, merge the blocks lay in level needed 
					 * to a higher level first.
					 * 
					 * Then, 
					 * copy all the latest data to this level,
					 * which creates a new block in this level
					 */
					b_s_new = absorbTo(level_needed);  
					
					//int blocks_count_at_target_level = this._vf_descriptor[level_needed][count_indicator];
					
					//if(blocks_count_at_target_level >= _vfs.levelTolerant(level_needed)
					//		&&  level_needed < _max_level)
					//{
					//	b_s_new = absorbTo(level_needed+1);  
					//}
					 
					
					/*
					 * After being absorbed, the handler block data 
					 * and all the blocks which are under the level of 
					 * level_needed have their data copied into  
					 * b_s_new. Therefore it becomes the latest block 
					 * of this virtual file, which means it has 
					 * no previous block, as well as a previous level. 
					 * Just evaluating the file handler to be b_s_new is 
					 * enough.
					 *  
					 */
					_handler_block.getMemReference().decreaseMemoryRef();
					_handler_block = b_s_new; 
				}  
				else
				{
					/*
					 * If level_needed equals to, or is smaller than, 
					 * the handler block's level, no jump happens.
					 * 
					 * And in this greedy mode, create new block 
					 * on the handler block's level, 
					 * and must check if the total blocks count is 
					 * bigger than the level tolerant count, 
					 * after this new block added to its 
					 * level.
					 * 
					 * If it is, merge the blocks in level_needed 
					 * to its upper level 
					 *  
					 */ 
					
						b_s_new = _vfs.acquirAvailableBlock(_handler_block.getMyLevel(), b_s_new); 
						if(b_s_new == null)
						{
							System.out.println("[Error]: appending a new block at level "
												+ _handler_block.getMyLevel() 
												+ " of the handler failed @appendDataGreedy for unknown reason.......");
							System.out.println((end - i_start + 1) 
										+ " records fail adding to a new block.");
							//return _handler_block;
							return i_end;
						}
					 
						/*
						 * @Step1: link to handler block
						 */
						 
						linkAToB(b_s_new, _handler_block);  
						/*
						 * @Step2: update the descriptor, merge if threshold 
						 * condition satisfied
						 */
						_handler_block.getMemReference().decreaseMemoryRef();
						_handler_block = updateDescriptorAfterAppending(b_s_new);
					//}
				} 
				this._current_level_manifold = _vfs.getLevelManifold(_handler_block);
				  
			} 
		}
		this.f_handler.handler_id = _handler_block.getID();
		this.f_handler.handler_level = _handler_block.getMyLevel();
 
		//return _handler_block;
		return end;
	}
	/*
	 *  returns true for succeed, false for failure
	 */
	public boolean appendData(int record) throws IOException 
	{ 
		int i_start = 0;
		int end = 0;
		
		BlockSimple _handler_block = this._vfs.getVFileBlock(this.f_handler, null);
		while(i_start<=end)
		{ 
			/*
			 * first fill up the handler, 
			 * it's the latest block within this submanifold 
			 */ 
			synchronized(_handler_block)
			{
				//int i_end = _handler_block.appendRecords(records, i_start, end-i_start+1);
				if(_handler_block.appendRecord(record))
					i_start += 1;  
				 
				if(i_start > end) 
					break; 
				/*
				 * for appending one record, always need a 
				 * level 0 block
				 */
				 
				int level_needed = 0;  
				
				BlockSimple b_s_new = null;
				  
				//b_s_new = _vfs.acquirAvailableBlock(level_needed); 
				b_s_new = _vfs.acquirAvailableBlock(level_needed, b_s_new); 
				if(b_s_new == null)
				{
					System.err.println("[Error]: appending a new block failed,"
										+ "	can not acquir a available block at level"
										+ level_needed );
					System.err.println("[Error]: record " + record + " failed adding to a new block.");
					//return _handler_block;
					return false;
				} 
				linkAToB(b_s_new, _handler_block);
				 
				_handler_block = updateDescriptorAfterAppending(b_s_new);
				 
			} 
			this._current_level_manifold = _vfs.getLevelManifold(_handler_block);
				  
		} 

		this.f_handler.handler_id = _handler_block.getID();
		this.f_handler.handler_level = _handler_block.getMyLevel();
 
		//return _handler_block;
		return true;
	}
	
	public boolean appendDataGreedy(int record) throws IOException 
	{ 
		int i_start = 0;
		int end = 0;
		
		BlockSimple _handler_block = this._vfs.getVFileBlock(this.f_handler, null);
		while(i_start<=end)
		{ 
			/*
			 * first fill up the handler, 
			 * it's the latest block within this submanifold 
			 */ 
			synchronized(_handler_block)
			{
				//int i_end = _handler_block.appendRecords(records, i_start, end-i_start+1);
				if(_handler_block.appendRecord(record))
					i_start += 1;  
				 
				if(i_start > end) 
					break; 
				
				/*
				 * In this greedy mode, create new block 
				 * on the handler block's level, 
				 * and must check if the total blocks count is 
				 * bigger than the level tolerant count, 
				 * after this new block add to its 
				 * level.
				 * 
				 * If it is, merge the blocks in level_needed 
				 * to its upper level 
				 *  
				 */ 
				 
				int level_needed =  _handler_block.getMyLevel();  
				
				BlockSimple b_s_new = null;
				  
				//b_s_new = _vfs.acquirAvailableBlock(level_needed); 
				b_s_new = _vfs.acquirAvailableBlock(level_needed, b_s_new); 
				if(b_s_new == null)
				{
					System.err.println("[Error]: appending a new block failed,"
										+ "	can not acquir a available block at handler level"
										+ level_needed );
					System.err.println("[Error]: record " + record + " failed adding to a new block.");
					//return _handler_block;
					return false;
				} 
				linkAToB(b_s_new, _handler_block);
				 
				_handler_block = updateDescriptorAfterAppending(b_s_new);
				 
			} 
			this._current_level_manifold = _vfs.getLevelManifold(_handler_block);
				  
		} 
		_handler_block.setPrev(-1);
		_handler_block.setPrevLevel(-1);
		this.f_handler.handler_id = _handler_block.getID();
		this.f_handler.handler_level = _handler_block.getMyLevel();
 
		//return _handler_block;
		return true;
	}
	
	public VFileHandler getHandler()
	{
		return this.f_handler;
	}
	
	public int getLevelCount()
	{
		return this._max_level+1;
	}
	
 
	
	private final boolean inRange(int _level)
	{
		return (_level >=0 && _level <=this._max_level);
	}
	
	/*
	 * patching from lower to upper(small to big)
	 */
	private final BlockSimple copyOnWritePatching(final int _from_level, final BlockSimple _target_level_block)
	{ 
		if(this._vf_descriptor[_from_level][count_indicator] == 0)
		{
			System.out.println("[INFO]: the level " + _from_level + " has no block to merge.");
			return _target_level_block;
		}
		int level_tolarant = this._vfs.levelTolerant(_from_level);
		if(level_tolarant <=0)
			return _target_level_block;
		
		int to_be_merged =(int) Math.min(level_tolarant, this._vf_descriptor[_from_level][count_indicator]);
		
		int i=0;
		BlockSimple oldest_block = null;
		while(i < to_be_merged)
		{ 
			long oldest_id = this._vf_descriptor[_from_level][oldest_indicator]; 
			if(oldest_id < 0)
			{
				oldest_block = null;
			}
			else
			{
				try {
					//oldest_block = this._vfs.getVFileBlock(_from_level, oldest_id);
					oldest_block = this._vfs.getVFileBlock(_from_level, oldest_id, oldest_block);
				} catch (IOException e) {
					System.err.println("[Error]: block " + oldest_id 
							+ " of virtual file at level" + _from_level 
							+ " is damaged, or somehow can not be loaded @VirtualFileInst.copyOnWritePatching(...)");
					e.printStackTrace();
					 
					//break;
				}
			}
			
			if(oldest_block == null)
			{
				System.err.println("[Error]: trying merge failed from level " 
							+ _from_level + ", and oldest block is " 
							+ oldest_id + " @VirtualFileInst.copyOnWritePatching, which should not be null.");
				System.err.println("[TRY]: try to recovery data from the level " 
						+ _from_level + ", and the " 
						+ (this._vf_descriptor[_from_level][count_indicator] - i-1) + "-th block"
						+ " @VirtualFileInst.copyOnWritePatching.");
				int j = 0;
				long cc = this._vf_descriptor[_from_level][count_indicator] - i ;
				BlockSimple t_block = null;
				boolean stop_at_j_th = false;
				long t_block_id =  this._vf_descriptor[_from_level][latest_indicator];
				while(j<cc && !stop_at_j_th)
				{ 
					try {
						t_block = this._vfs.getVFileBlock(_from_level, t_block_id, t_block);
						if(t_block != null)
						{
							oldest_block = t_block;
							if(t_block.getNextLevel() == _from_level)
							{
								t_block_id = t_block.getNext();
								j++;
							}
							else
								stop_at_j_th = true;
						}
						else
							stop_at_j_th = true;
							
					}
					catch (IOException e) {
						System.err.println("[Error]: failed reading block "
										+ t_block_id 
										+ " at level " 
										+ _from_level 
										+ " in recovering @VirtualFileInst.copyOnWritePatching(...)");
						e.printStackTrace(); 
						stop_at_j_th = true;
					}
				}
				if(oldest_block == null) 
					break;
			}
			synchronized(oldest_block)
			{
				_target_level_block.appendRecords(oldest_block.getData(), 0, oldest_block.getRecCount());
				
				_target_level_block.setPrev(oldest_block.getPrev());
				_target_level_block.setPrevLevel(oldest_block.getPrevLevel());
				/*
				System.out.println("Succeed copying block " + oldest_block.getID() 
						+ " at level " + oldest_block.getMyLevel() 
						+ " to block " + _target_level_block.getID()
						+ " at level " + _target_level_block.getMyLevel());
				System.out.println("Now update file description information of level "
						+ _from_level);
				*/
				this._vf_descriptor[_from_level][oldest_indicator] = oldest_block.getPrev(); 
				this._vf_descriptor[_from_level][count_indicator]--; 
				if(this._vf_descriptor[_from_level][count_indicator] == 0)
				{
					this._vf_descriptor[_from_level][latest_indicator] = -1; 
					this._vf_descriptor[_from_level][oldest_indicator] = -1; 
				}
				
				this.block_garbage.add((long)oldest_block.getMyLevel());
				this.block_garbage.add(oldest_block.getID());
			}
			i++; 
		}
		 
		//no matter the from_level has blocks remain or not.
		if(oldest_block!=null)
		{
			_target_level_block.setPrev(oldest_block.getPrev());
			_target_level_block.setPrevLevel(oldest_block.getPrevLevel());
		}
		return _target_level_block;
	} 
	
	/*
	 * patch once, and let the application decide whether to patch the 
	 * remaining or just stop. 
	 * Inside is a copy on write strategy.
	 * Returns the latest block of the next(target) level,
	 * which contains the data from the blocks lay on the lower level.
	 */ 
	@Deprecated
	public BlockSimple lazyPatch(int _level) throws IOException 
	{  
		if(_level == this._max_level)
			return null;   
		
		int target_level = _level+1;
		//BlockSimple _target_adjacent_level_block = this._vfs.acquirAvailableBlock(target_level);
		BlockSimple _target_adjacent_level_block = null;
		_target_adjacent_level_block = this._vfs.acquirAvailableBlock(target_level, _target_adjacent_level_block);
		  
		_target_adjacent_level_block = this.copyOnWritePatching(_level, _target_adjacent_level_block);
		
		//get the original latest block in target level, 
		//and link the target block to it
		//The following is an atomic transaction to keep the links within a 
		//virtual file CONSISTENTLY. 
		try
		{
			long origin_latest = this._vf_descriptor[target_level][0]; 
			if(origin_latest != -1)
			{
				//@step 1:
				BlockSimple origin_block = null;
				origin_block = this._vfs.getVFileBlock(target_level, origin_latest, origin_block);
				_target_adjacent_level_block.setNextLevel(origin_block.getMyLevel());
				_target_adjacent_level_block.setNext(origin_block.getID()); 
				//@step 2:
				this._vfs.writeVFileBlock(_target_adjacent_level_block);
				//@step 3:
				origin_block.setPrev(_target_adjacent_level_block.getID());
				origin_block.setPrevLevel(origin_block.getMyLevel());
				this._vfs.writeVFileBlock(origin_block);
				//@step 4:
				//Succeed flushing to the disk, then update the descriptor 
				this._vf_descriptor[target_level][1]++;
				this._vf_descriptor[target_level][0] = _target_adjacent_level_block.getID();
			}
			else
			{
				this._vfs.writeVFileBlock(_target_adjacent_level_block);
				this._vf_descriptor[target_level][1]=1;
				this._vf_descriptor[target_level][0] = _target_adjacent_level_block.getID();
				this._vf_descriptor[target_level][2] = _target_adjacent_level_block.getID();
			}
			
			return _target_adjacent_level_block;
			
		}
		catch(IOException e)
		{
			System.out.println("Fail patching blocks from level " + _level + " to level " + target_level);		
			//return null, just like nothing happened.
			return null;
		}
	}
	
	/*
	 * patching the lower level blocks' data into the target_level,
	 * the block returned contains the latest data of this virtual file.
	 */
	private final BlockSimple absorbTo( int target_level) throws IOException 
	{   
		if(!inRange(target_level))
			return null;   
		
		BlockSimple _target_block = null;
		for(int current_level = target_level-1; current_level >=0; current_level --)
		{
			while(this._vf_descriptor[current_level][count_indicator]>0)
			{
				/*
				 * then the target block is full in this loop
				 * (otherwise the _vf_descriptor[current_level][1] is 0), 
				 * generate a new one at the same target level to store 
				 * the remaining of current level. 
				 *
				 */ 
				//if(_target_block!=null)
				//	_target_block.getMemReference().decreaseMemoryRef();
					//this._vfs.getLevelManifold(_target_block.getMyLevel()).returnBlock(_target_block);
				
				//_target_block = this._vfs.acquirAvailableBlock(target_level); 
				_target_block = this._vfs.acquirAvailableBlock(target_level,_target_block); 
				 
				synchronized(_target_block)
				{
					_target_block = this.copyOnWritePatching(current_level, _target_block);
					try
					{
						/* 
						 * get the original latest block in target level, 
						 * and link the target block to it.
						 * If the target level does not has a block, 
						 * seek its upper level, till find one or reaches the max level.
						 * 
						 * The following is an atomic transaction to keep the links within a
						 * virtual file CONSISTENTLY.  
						 */ 
						long origin_latest = -1; 
						int _next_level = target_level;
						while( _next_level <= _max_level )
						{
							origin_latest = this._vf_descriptor[_next_level][latest_indicator]; 
							if(origin_latest == -1)
								_next_level++;
							else
								break;
						}
						/*
						 * there is already an original latest block, 
						 * on _next_level 
						 */
						if(origin_latest != -1  )
						{
							//@step 1:
							//BlockSimple origin_block = this._vfs.getVFileBlock(_next_level, origin_latest);
							BlockSimple origin_block = null;
							origin_block = this._vfs.getVFileBlock(_next_level, origin_latest, origin_block);
							if(origin_block != null)
							{
								_target_block.setNextLevel(origin_block.getMyLevel());
								_target_block.setNext(origin_block.getID()); 
								//@step 2:
								this._vfs.writeVFileBlock(_target_block);
								//@step 3:
								origin_block.setPrev(_target_block.getID());
								//origin_block.setPrevLevel(origin_block.getMyLevel());
								origin_block.setPrevLevel(_target_block.getMyLevel());
								this._vfs.writeVFileBlock(origin_block); 
								//@step 4:
								//Succeed flushing to disk, then update the descriptor 
								this._vf_descriptor[target_level][count_indicator]++;
								this._vf_descriptor[target_level][latest_indicator] = _target_block.getID();
								if(this._vf_descriptor[target_level][count_indicator] == 1
									&& target_level < _max_level)
								{
									this._vf_descriptor[target_level][oldest_indicator] = _target_block.getID();
								}
								//@step 5:
								//decrease the reference count of the origin block
								origin_block.getMemReference().decreaseMemoryRef();
								if(this._vf_descriptor[target_level][count_indicator] >= _vfs.levelTolerant(target_level)
										&& target_level < _max_level)
								{
						 
									/*
									 * if recursively absorb, first return the target block 
									 * to file system(decrease the reference)
									 */
									//this._vfs.getLevelManifold(_target_block.getMyLevel()).returnBlock(_target_block);
									BlockSimple target_block_backup = _target_block; 
									_target_block.getMemReference().decreaseMemoryRef();
									_target_block = absorbTo(target_level+1);
									if(_target_block == null)
									{	
										_target_block = target_block_backup;
										_target_block.getMemReference().increaseMemoryRef();
									}
								}
							}
							else
							{
								/*
								 * if the original latest block is null(damaged or sth else),
								 * just update the descriptor 
								 */
								_target_block.setNextLevel(-1);
								_target_block.setNext(-1);  
								this._vfs.writeVFileBlock(_target_block); 
								this._vf_descriptor[target_level][count_indicator] = 1;
								this._vf_descriptor[target_level][latest_indicator] = _target_block.getID();
								if(this._vf_descriptor[target_level][count_indicator] == 1
									&& target_level < _max_level)
								{
									this._vf_descriptor[target_level][oldest_indicator] = _target_block.getID();
								}
							}
						}
						/*
						 * the target block is the first block in this level, 
						 * and all the upper levels. 
						 */
						else
						{ 
							_target_block.setNextLevel(-1);
							_target_block.setNext(-1);
							this._vfs.writeVFileBlock(_target_block);
							this._vf_descriptor[target_level][count_indicator]=1;
							this._vf_descriptor[target_level][latest_indicator] = _target_block.getID();
							this._vf_descriptor[target_level][oldest_indicator] = _target_block.getID();
						
						}  
					}
					catch(IOException e)
					{
						System.err.println("[ERROR]: Fail patching blocks from level " + current_level + " to level " + target_level 
								+ "@VirtualFileInst.absorbTO()");		
						/*
						 * return null, just like nothing happened.
						 */
						_target_block.setAvailable();
						this._vfs.blockGarbo(_target_block.getID(), _target_block.getMyLevel());
						this._vfs.getLevelManifold(_target_block.getMyLevel()).returnBlock(_target_block);
						this.block_garbage.clear();
						return null;
					}
					/*  
					 * do not remove the garbage here, since the _vf_descriptor 
					 * and handler information has not been saved yet. 
					 * 
					 * If system failure occurs and Lunarbase restarts, we still has only the 
					 * older handler and _vf_descriptor. 
					 */
					/*
					if(this._vfs.blockGarbo(this.block_garbage))
						this.block_garbage.clear();
					else
					{
						System.out.print("[Warning]: there are disk garbage left, the system has some exceptions in cleansing it");
						//To Do: log this, and prompt user to use external 
						//tool to clean these garbage blocks.
						 
						this.block_garbage.clear();
					}
					*/
				}
			} 
		}
		
		/*
		 * finish patching all the blocks under the target level.
		 * returns the block on target_level. 
		 */ 
		
		return _target_block; 
	}
	
	public void garbo()
	{
		if(this._vfs.blockGarbo(this.block_garbage))
			this.block_garbage.clear();
		else
		{
			System.out.print("[Warning]: there are disk garbage left, the system has some exceptions in cleansing it");
			//To Do: log this, and prompt user to use external 
			//tool to clean these garbage blocks.
			 
			this.block_garbage.clear();
		}
	}
	
	/*
	 * return all its blocks to the pool, 
	 * which means those blocks can be eliminated in 
	 * block cache LRU competition 
	 */
	public void close()
	{
		
	}
}

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

package LCG.FSystem.AtomicStructure;

import java.io.IOException;
 
import LCG.FSystem.Def.DBFSProperties; 
import LCG.FSystem.Manifold.LFSBaseLayer; 
import LCG.StorageEngin.IO.L0.IOInterface; 
import LCG.StorageEngin.IO.L1.IOStreamNative;
 
import LCG.StorageEngin.Serializable.Impl.Int4Byte;
 
public class BlockGeneric<H extends EntryAbstract> extends IFSObject{ 
	protected final H s_v_entry;
	final int[] s_records;  
	 
	final Int4Byte g_int_rw = new Int4Byte(); 
	int memory_used_offset = 0; 
	 
	protected final DBFSProperties dbfs_prop_instance;
	public BlockGeneric(H _entry, int index_in_pool, DBFSProperties _dbfs_prop_instance)  
	{
		super(index_in_pool);
		s_v_entry = _entry;  
		s_records = new int[calculateRecAllowed()];
		dbfs_prop_instance = _dbfs_prop_instance;
	}
	public BlockGeneric(H _entry, DBFSProperties _dbfs_prop_instance) 
	{  
		s_v_entry = _entry;  
		s_records = new int[calculateRecAllowed()];
		dbfs_prop_instance = _dbfs_prop_instance;
	}
	
	private int calculateRecAllowed() throws IndexOutOfBoundsException
	{
		int counts = (int)((s_v_entry.getBlockSize() - s_v_entry.sizeInBytes())/g_int_rw.sizeInBytes());
		if(counts <= 0)
		{
			throw new IndexOutOfBoundsException("Error: Block size should bigger than its entry size. Creating new block failed.");
		}
		return counts;
	} 
	
	public int sizeInBytes()
	{
		return s_v_entry.getBlockSize(); 
	} 
	
	public int getMyLevel()
	{
		return s_v_entry.getBlockLevel();
	}
	
	public boolean isDirt()
	{
		return this.s_v_entry.isDirt();
	}
	private void setClean()
	{
		this.s_v_entry.setClean();
	}
	
	public boolean appendRecord(int record_id) 
	{
		synchronized(this)
		{
			if((DataBeginOffset() + s_v_entry.s_saved_offset + memory_used_offset + g_int_rw.sizeInBytes()) <= BlockEndOffset())
			{ 
				s_records[s_v_entry.s_rec_count] = record_id;
				memory_used_offset +=  g_int_rw.sizeInBytes();
				s_v_entry.s_rec_count ++;
				s_v_entry.is_dirt = true; 
				return true;
			}
			else
				return false;
		}
	}

	//returns the position the last element copied 
	public int appendRecords(int[] records, int from, int length) 
	{ 
		synchronized(this)
		{
			int used = s_v_entry.s_saved_offset + memory_used_offset;
			int count = length;
			int remains = (int)((BlockEndOffset() - DataBeginOffset() - used)/g_int_rw.sizeInBytes());
			int allowed = Math.min(remains, count);
			System.arraycopy(records, from, s_records, s_v_entry.s_rec_count, allowed);
			memory_used_offset +=  allowed * g_int_rw.sizeInBytes();
			s_v_entry.s_rec_count += allowed;
			s_v_entry.is_dirt = true; 
			return from+allowed-1; 
		}
	}
	
	public long BlockBeginOffset()
	{ 
		return (long) this.s_v_entry.getMyPosition() % dbfs_prop_instance.seg_file_size;
	}
	
	public long DataBeginOffset()
	{
		return (long) BlockBeginOffset() + s_v_entry.sizeInBytes() ;
	}
	
	public long BlockEndOffset()
	{
		return (long) (BlockBeginOffset() + s_v_entry.getBlockSize()-1);
	}
	
	public void ReadEntry(IOInterface io_v) throws IOException
	{
		synchronized(this)
		{
			synchronized(io_v)
			{
				s_v_entry.Read(io_v);
				this.s_obj_id = s_v_entry.getID();
			}
			
		}
	}
	
	protected H getEntry()
	{
		synchronized(this.s_v_entry)
		{
			return this.s_v_entry;
		}
	}
	
	public void ReadData(IOInterface io_v) throws IOException
	{
		synchronized(this)
		{
			synchronized(io_v)
			{
				int data_size = (int)(this.BlockEndOffset() - this.DataBeginOffset() +1); 
				//byte[] block_data = new byte[data_size];
				//io_v.read(block_data);
				int t_used_offset = 0;
		 
				if(io_v.isNative())
				{
					((IOStreamNative)io_v).read(this.s_records, 0, s_v_entry.s_rec_count);
					t_used_offset = 4 * s_v_entry.s_rec_count;
				}
				else
				{
					for(int i=0; i<s_v_entry.s_rec_count; i++)
					{ 
						//this.s_records[i] = io_v.Transform4ByteToInt(block_data, i*4);
						this.s_records[i] = io_v.read4ByteInt();
						t_used_offset +=4;
					} 
				} 
			 
				if(t_used_offset != s_v_entry.s_saved_offset)
				{
					System.out.println("Warning: block "
		                + s_obj_id + " has incorrect records count in it");
					s_v_entry.s_saved_offset = t_used_offset;
				}  
		 
				io_v.seek(BlockEndOffset()+1); 
			}
		}
	}
	@Override
	public void Read(IOInterface io_v) throws IOException { 
		ReadEntry(io_v);
		ReadData(io_v); 
	}
	
	@Override
	public void Write(IOInterface io_v) throws IOException {
		synchronized(this)
		{
		synchronized (io_v) {
			//if has new records in memory, then flushes it to disk
			if(s_v_entry.isDirt())
			{ 
				s_v_entry.setID(this.s_obj_id);
				s_v_entry.s_saved_offset +=memory_used_offset; 
				s_v_entry.Write(io_v);
				 
				long new_data_begin_pos = DataBeginOffset() + s_v_entry.s_saved_offset - memory_used_offset; 
				io_v.seek(new_data_begin_pos);  
				
				int begin_index = (int) ((s_v_entry.s_saved_offset - memory_used_offset) / this.g_int_rw.sizeInBytes());
				int count = (int)(memory_used_offset / this.g_int_rw.sizeInBytes()); 
				 
				if(io_v.isNative())
				{
					((IOStreamNative)io_v).write(this.s_records, begin_index, count);
				}
				else
				{
					for(int i =0;i<count;i++)
					{
						io_v.write4ByteInt(this.s_records[begin_index+i]);
					} 
				}
				
				
				memory_used_offset = 0;
				this.setClean();
				io_v.seek(BlockEndOffset()+1);
			}
			else
			{
				io_v.seek(BlockEndOffset()+1); 
			}
		}
		}
	} 
	
	@Override
	public void setID(long id)
	{
		synchronized(this)
		{
			this.s_obj_id = id;
			this.s_v_entry.setID(id); 
			this.s_v_entry.is_dirt = true;
		}
		
	}
	
	public Byte getAvailability()
	{
		return this.s_v_entry.available;
	}
	  
	public int recElementBytes()
	{
		return g_int_rw.sizeInBytes();
	}
	
	public int recAllowed()
	{
		return this.s_records.length;
	}
	
	public int getRecCount()
	{
		return this.s_v_entry.s_rec_count;
	}
	
	public int[] getData()
	{
		return this.s_records;
	}
	
	public int getRecordAt(int index)
	{
		if(index<0 || index >= this.s_v_entry.s_rec_count)
			return -1;
		else
			return this.s_records[index];
	} 
	
	/*
	 * release the memory occupied in pool.
	 * all the information in this block is removed, including block id. 
	 * The things kept unchanged are its reference in memory pool, 
	 * block size, and its level.
	 * See the implementation of its entry counterpart: s_v_entry.returnToPool()
	 * 
	 */
	 
	public void returnToPool()  
	{ 
		synchronized(this)
		{
			this.s_v_entry.returnToPool(); 
			this.s_obj_id = -1; 
			this.memory_used_offset = 0;
		}
	} 
	
	/*
	 * set available, and all the data and links are cleared,
	 * only the block id, size and level remain.
	 * After setting available, 
	 * it is ready to be used by other virtual file
	 */
	 
	public void setAvailable()
	{
		synchronized(this)
		{
			this.s_v_entry.setAvailable();  
			this.memory_used_offset = 0;
		}
	}
	
	public void setUnavailable()
	{
		synchronized(this)
		{
			this.s_v_entry.setUnavailable();  
		}
	}
 

	public int getEntrySize()
	{
		return this.s_v_entry.sizeInBytes();
	}
	
	public long getMyPosition()
	{
		return this.s_v_entry.getMyPosition();
	}
	
	public void setMyPosition(long _pos)
	{
		this.s_v_entry.setMyPosition(_pos);
	}
	
	
	
	@Override
	public void read(LFSBaseLayer al_file) throws IOException{
		  
		int index = al_file.getStreamIndex(this.s_v_entry.getMyPosition());
		long offset = al_file.getOffset(this.s_v_entry.getMyPosition());
		if(offset != this.BlockBeginOffset())
			throw new IOException("[Error]: the block size may not match, check again");
		
		IOInterface t_io = (IOInterface)al_file.cardinates.get(index);  
		
		synchronized (t_io) { 			
			t_io.seek(offset); 
			this.Read(t_io);  
		}  
	}

	@Override
	public void write(LFSBaseLayer tsf_file) throws IOException {	 
		int t_index = tsf_file.getStreamIndex(this.s_v_entry.getMyPosition());
		long offset = tsf_file.getOffset(this.s_v_entry.getMyPosition());
		IOInterface t_io = (IOInterface)tsf_file.cardinates.get(t_index); 
		synchronized (t_io) { 
			t_io.seek(offset); 
			this.Write(t_io);   
		} 
	}
 

}

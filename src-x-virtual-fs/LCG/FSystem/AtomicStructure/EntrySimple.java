/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author DADA database team 
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


package LCG.FSystem.AtomicStructure;

import java.io.IOException;

import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.FSystem.Manifold.TManifold;
import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.IO.L1.IOStream; 
import LCG.StorageEngin.Serializable.IObject;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;

public class EntrySimple extends EntryAbstract{
	
	public BlockSizes block_size; 
	public int _my_level;
	public long s_previous_id;
	public int s_prev_level;
	public long s_next_id; 
	public int s_next_level;
	public int s_id_within_level;
	
	/*
	 * @deprecated
	public EntrySimple()
	{ 
		//default construction constructs a 4k block
		block_size = BlockSizes.block_4k;
		s_status = (byte)((int)BlockFlages.unavailable + BlockFlages.getLevel(block_size));
		available = BlockFlages.unavailable;
		_my_level = BlockFlages.getLevel(block_size);
		s_obj_id = -1;
		s_previous_id = -1;
		s_next_id = -1; 
		s_id_within_level = -1;
		s_rec_count = 0; 
		s_used_offset = 0;
		is_dirt = false;  
		
		byte_buff = new byte[Size()]; 
	}
	*/
	
	
	public EntrySimple(BlockSizes b_s)
	{
		s_status = (byte)((int)DBFSProperties.unavailable + b_s.getLevel());
		
		available = DBFSProperties.unavailable;  
		block_size =  b_s; 
		_my_level = DBFSProperties.getLevel(block_size);
		
		s_obj_id = -1;
		
		s_previous_id = -1;
		s_prev_level = -1;
		s_next_id = -1; 
		s_next_level = -1; 
		
		s_id_within_level = -1;
		s_rec_count = 0; 
		s_saved_offset = 0;
		is_dirt = true; 
		
		byte_buff = new byte[sizeInBytes()]; 
	 
	}
	
	/*
	 * @deprecated
	public EntrySimple(int id, int pre_id, int next_id, BlockSizes b_size)
	{
		this.available = BlockFlages.unavailable;
		this.block_size = b_size; 
		this._my_level = BlockFlages.getLevel(block_size);
		this.s_obj_id = id;
		this.s_previous_id = pre_id;
		this.s_next_id = next_id;
		this.s_rec_count = 0; 
		this.s_used_offset = 0;
		is_dirt = true;
	}
	*/
	public int sizeInBytes()
	{ 
		return 1 + super.sizeInBytes() + 4*4+2*8 +4 ;
	}
	
	public void Read(IOInterface io_v) throws IOException
	{ 
        if(io_v.read(byte_buff)!=-1)
        {
        	this.s_status = byte_buff[0];
        	this.available = DBFSProperties.getAvailability((int)s_status);
        	this.block_size = DBFSProperties.getBlockSize((int)s_status);
        	this._my_level = DBFSProperties.getLevel(block_size);
        	
        	this.s_obj_id = VariableGeneric.Transform8ByteToLong(byte_buff,1);
        	this.s_previous_id = VariableGeneric.Transform8ByteToLong(byte_buff,9);
        	this.s_prev_level = VariableGeneric.Transform4ByteToInt(byte_buff,17);
        	
        	this.s_next_id = VariableGeneric.Transform8ByteToLong(byte_buff,21);
        	this.s_next_level = VariableGeneric.Transform4ByteToInt(byte_buff,29);
        	
        	this.s_id_within_level = VariableGeneric.Transform4ByteToInt(byte_buff,33);
        	this.s_rec_count = VariableGeneric.Transform4ByteToInt(byte_buff,37);
        	this.s_saved_offset = VariableGeneric.Transform4ByteToInt(byte_buff,41); 
        	is_dirt = false;
        }
        else
        {
        	//System.out.println("[ERROR]: @EntrySimple.Read(IOInterface io_v), read the end of the block entry file. This block may be lost.") ;
        	throw new IOException("[ERROR]: @EntrySimple.Read(IOInterface io_v), read the end of the block entry file. This block may be lost.");
        } 
	} 
 
	public void Write(IOInterface io_v) throws IOException
	{ 
		if(is_dirt)
		{
			io_v.write(this.s_status);
			io_v.WriteLong(s_obj_id); 
			io_v.WriteLong(s_previous_id);
			io_v.write4ByteInt(s_prev_level); 
        	
			io_v.WriteLong(s_next_id);
			io_v.write4ByteInt(s_next_level);
			 
			io_v.write4ByteInt(s_id_within_level);
			io_v.write4ByteInt(s_rec_count);
			io_v.write4ByteInt(s_saved_offset);
		} 
	}  
	
	//set a block to be available, remove all its information, 
	//including its object id
	//but does not change the block size, hence its level.
	//object id cleared, hence the flag dirt is false.
	public void returnToPool()  
	{ 
		this.s_obj_id = -1; 
		setAvailable();		
		this.is_dirt = false; 
	}
	
	//set available to use by other virtual file.
	//This function just breaks all the links it has, 
	//and all the data indicator to be the default ones.
	//The object id, size and level remain.
	//Set to be dirt, if it is flushed to storage, 
	//the change of the status will be saved for future use.
	public void setAvailable()
	{ 
		this.s_status = (byte)((int)DBFSProperties.available + DBFSProperties.getLevel(this.block_size)); //set available
		this.available = DBFSProperties.available;
		this.s_previous_id = -1;
		this.s_prev_level = -1;
		
		this.s_next_id = -1;
		this.s_next_level = -1;
		
		this.s_id_within_level = -1;
		this.s_rec_count = 0;
		this.s_saved_offset = 0; 
		
		this.is_dirt = true; 
	}
	
	public void setUnavailable()
	{ 
		this.s_status = (byte)((int)DBFSProperties.unavailable + DBFSProperties.getLevel(this.block_size)); //set available
		this.available = DBFSProperties.unavailable; 
	}
 
	 
	public void setID(long id) {
		this.s_obj_id = id;
		this.is_dirt = true;
		
	}  
	
	public boolean isDirt()
	{
		return this.is_dirt;
	}


	@Override
	public int getBlockSize() {
		return this.block_size.getSize();
	}

	public int getBlockLevel()
	{
		return this.block_size.getLevel();
	}

	@Override
	public void setClean() {
		this.is_dirt = false;
		
	}


	@Override
	public long getMyPosition() { 
		return (long)(this.s_obj_id * this.block_size.getSize());
	}
	
	@Override
	public void setMyPosition(long pos) {
		//for entry simple, which has fixed block size, 
		//can not set self position. the only way to change 
		//position is to set ID. Then it will get its position 
		//just as getMyPosition() does
	}


	@Override
	public Byte getAvailability() {
		return this.available;
	}


	@Override
	public int expandBlockSize(int new_size) {
		//for entry simple, which has fixed block size, 
		//do not apply this interface
		return 0;
	}
}

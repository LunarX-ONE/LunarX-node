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

//BlockTail stores the data fragments which can not fill up a 
//complete 4K (or other somehow customized size) block. 
//Then put it here.
//Always the latest data is in this.

package LCG.FSystem.UnSure;

import java.io.IOException;

import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.FSystem.AtomicStructure.EntryAbstract;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.FSystem.Manifold.TManifold;
import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.IO.L1.IOStream; 
import LCG.StorageEngin.Serializable.IObject;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;

public class EntryV extends EntryAbstract{
	 
	public int s_vb_size; //size of this variant block
	
	public long s_my_pos;
	public long s_previous_pos;
	public long s_next_pos; 
	
	public EntryV()
	{
		s_status = (byte)((int)DBFSProperties.available);
		
		available = DBFSProperties.available;
		s_obj_id = -1;
		
		s_vb_size =  0; 
		s_my_pos = -1;
		s_previous_pos = -1;
		s_next_pos = -1; 
		s_rec_count = 0; 
		s_saved_offset = 0;
		is_dirt = false; 		
		byte_buff = new byte[sizeInBytes()]; 
	}
	
	public EntryV(int _size)
	{
		s_status = (byte)((int)DBFSProperties.available);
		
		available = DBFSProperties.available;
		s_obj_id = -1;
		
		s_vb_size =  _size; 
		s_my_pos = -1;
		s_previous_pos = -1;
		s_next_pos = -1; 
		s_rec_count = 0; 
		s_saved_offset = 0;
		is_dirt = true; 
		
		byte_buff = new byte[sizeInBytes()]; 
	 
	}
	
	
	public EntryV(int _id, long _my_pos, long _pre_pos, long _next_pos, int _size)
	{
		this.available = DBFSProperties.available;
		this.s_vb_size = _size; 
		this.s_obj_id = _id;
		this.s_my_pos = _my_pos;
		this.s_previous_pos = _pre_pos;
		this.s_next_pos = _next_pos;
		this.s_rec_count = 0; 
		this.s_saved_offset = 0;
		is_dirt = true;
	}
	
	public int sizeInBytes()
	{
		//1 byte, 2 long and 2 int in this class 
		return 1 + super.sizeInBytes() + 4 + 8*3 + 4*2 ;
	}
	
	public void Read(IOInterface io_v) throws IOException
	{
		synchronized(this)
		{
        if(io_v.read(byte_buff)!=-1)
        {
        	this.s_status = byte_buff[0];
        	this.available = DBFSProperties.getAvailability((int)s_status);
        	 
        	this.s_obj_id = VariableGeneric.Transform8ByteToLong(byte_buff,1);
        	this.s_vb_size = VariableGeneric.Transform4ByteToInt(byte_buff,9);
        	this.s_my_pos = VariableGeneric.Transform8ByteToLong(byte_buff,17);
        	this.s_previous_pos = VariableGeneric.Transform8ByteToLong(byte_buff,25);
        	this.s_next_pos = VariableGeneric.Transform8ByteToLong(byte_buff,33);
        	this.s_rec_count = VariableGeneric.Transform4ByteToInt(byte_buff,37);
        	this.s_saved_offset = VariableGeneric.Transform4ByteToInt(byte_buff,41); 
        	is_dirt = false;
        }
        else
        {
        	throw new IOException("read end of the block info file. This block may be lost: " + s_obj_id);
        } 
		}
	} 
 
	public void Write(IOInterface io_v) throws IOException
	{ 
		synchronized(this)
		{
		if(is_dirt)
		{
			io_v.write(this.s_status);
			io_v.WriteLong(s_obj_id);  
			io_v.write4ByteInt(s_vb_size);
			io_v.WriteLong(s_my_pos);
			io_v.WriteLong(s_previous_pos);
			io_v.WriteLong(s_next_pos);
			io_v.write4ByteInt(s_rec_count);
			io_v.write4ByteInt(s_saved_offset);
			
		} 
		}
	}  
	
	//set a block to be available, while the size and self position do not change
	public void returnToPool()  
	{ 
		synchronized(this)
		{
		this.s_obj_id = -1;
		this.s_status = (byte)((int)DBFSProperties.available); //set available 
	 	
		this.s_previous_pos = -1;
		this.s_next_pos = -1;
		this.s_rec_count = 0;
		this.s_saved_offset = 0;  
		this.is_dirt = false; 
		}
	}


	 

	 
	 
	public void setID(int id) {
		synchronized(this)
		{
			this.s_obj_id = id;
			this.is_dirt = true;
		}
		
	}  
	
	public boolean isDirt()
	{
		synchronized(this)
		{
			return this.is_dirt;
		}
	}


	@Override
	public int getBlockSize() { 
		synchronized(this)
		{
			return this.s_vb_size;
		}
	}
	

	@Override
	public int expandBlockSize(int new_size) { 
		synchronized(this)
		{
			this.s_vb_size += new_size;
			return this.s_vb_size;
		}
		
	}
	
	public void setClean()
	{
		synchronized(this)
		{
			this.is_dirt = false;
		}
	}


	@Override
	public long getMyPosition() {
		synchronized(this)
		{
			return this.s_my_pos;
		}
	}
	
	public void setMyPosition(long _pos) {
		synchronized(this)
		{
			this.s_my_pos = _pos;
		}
		
	}
	public Byte getAvailability()
	{
		return available;
	}

	@Override
	public int getBlockLevel() {  
		//the block with variant size has no level definition.
		return -1;
	}

	@Override
	public void setAvailable() {
		synchronized(this)
		{ 
			this.s_status = (byte)((int)DBFSProperties.available); //set available 
	 	
			this.s_previous_pos = -1;
			this.s_next_pos = -1;
			this.s_rec_count = 0;
			this.s_saved_offset = 0;  
			this.is_dirt = true; 
		}
		
	}

	@Override
	public void setUnavailable() {
		this.s_status = (byte)((int)DBFSProperties.unavailable ); //set available
		this.available = DBFSProperties.unavailable; 
		
	}

	@Override
	public void setID(long _id) {
		this.s_obj_id = _id;
		this.is_dirt = true;
		
	}

}

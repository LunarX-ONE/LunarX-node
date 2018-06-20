/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author DADA database team, feiben@lunarion.com
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

package Lunarion.SE.AtomicStructure;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import LCG.FSystem.CopyOnWrite.VirtualFileInst;
import LCG.FSystem.CopyOnWrite.VirtualFileSystem;
import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.IO.L1.IOStreamNative;
import LCG.StorageEngin.Serializable.IObject; 
import LCG.StorageEngin.Serializable.Impl.String256CharsNew;
import LunarX.Node.Conf.EnginPathDefinition;
import Lunarion.SE.HashTable.Stores.StoreRowEntries;

/* Entry for each row.
 * Each row is quite similar to a file in a file system,
 * which is stored as linked physical blocks on disk.
 */

public class RowEntry extends IObject {

	//int[] s_entry_data = new int[4];
	private long s_key_id;
	//int[] s_entry_data =  new int[3];
	
	//private final int vf_handler_level_indicator = 0;
	//private final int vf_handler_id_indicator = 1;
	// how many records in this row
	//private final int rec_count_indicator = 2;
	
	private int vf_handler_level = -1;
	private long vf_handler_id = -1;
	
	private long[][] vf_discriptor; 
	private VirtualFileInst vf_inst;
	private int[] id_buff ;
	private int buff_len = 200;
	private int id_count_in_buff;
	
	private int level_count;
	private static int discriptor_collumn_dim = VirtualFileInst.descriptor_dim;
	// how many records in this row
	private int s_rec_count = 0;

	/*
	 * where is this entry located. this variable is not serializable, it is
	 * read from EnginPathDefinition.row_entry_address_dir
	 */
	public long _my_position;
	public String256CharsNew s_key_string;

	
	private boolean is_dirty;

	/*
	 * row entry must be created by knowing itself where it is located.
	 */
	public RowEntry(long _position) {

		s_key_id = -1;
		//s_entry_data[vf_handler_level_indicator] = -1;
		//s_entry_data[vf_handler_id_indicator] = -1;
		//s_entry_data[rec_count_indicator] = 0;
		
		vf_handler_level  = -1;
		vf_handler_id = -1;
		s_rec_count = 0;
		
		id_buff = new int[buff_len];
		id_count_in_buff = 0;
		
		s_key_string = new String256CharsNew();
		_my_position = _position;
		is_dirty = false;

	}

	// public RowEntry(int _key_id, int _vf_handler_level, int _vf_handler_id,
	// int record_count, long position)
	@Deprecated
	public RowEntry(long _key_id, VirtualFileSystem _vfs, long position) throws IOException {
		vf_inst = new VirtualFileInst(_vfs);

		s_key_id = _key_id;
		//s_entry_data[vf_handler_level_indicator] = vf_inst.getHandler().handler_level;
		//s_entry_data[vf_handler_id_indicator] = vf_inst.getHandler().handler_id;
		//s_entry_data[rec_count_indicator] = 0;
 
		vf_handler_level =  vf_inst.getHandler().handler_level;
		vf_handler_id = vf_inst.getHandler().handler_id;
		// how many records in this row
		s_rec_count  = 0;
	}

	public RowEntry(long position, long _key_id, VirtualFileSystem _vfs, String str_val) throws IOException {
		vf_inst = new VirtualFileInst(_vfs);

		s_key_id = _key_id;
		//s_entry_data[vf_handler_level_indicator] = vf_inst.getHandler().handler_level;
		//s_entry_data[vf_handler_id_indicator] = vf_inst.getHandler().handler_id;
		//s_entry_data[rec_count_indicator] = 0;
		vf_handler_level  = vf_inst.getHandler().handler_level;
		vf_handler_id = vf_inst.getHandler().handler_id;
		vf_discriptor = vf_inst.getVFDiscriptor();
		level_count = vf_inst.getLevelCount();
		 
		id_buff = new int[buff_len];
		id_count_in_buff = 0;
		
		s_rec_count = 0;
				
		s_key_string = new String256CharsNew(str_val);
		this._my_position = position;
		
		is_dirty = true;
	}

	@Override
	public void Read(IOInterface io_v) throws IOException {
		synchronized (io_v) {

			if (io_v.isNative()) {
				s_key_id = io_v.ReadLong();
				//((IOStreamNative) io_v).read(s_entry_data, 0, s_entry_data.length);
				vf_handler_level = io_v.read4ByteInt();
				vf_handler_id = io_v.ReadLong();
				s_rec_count =  io_v.read4ByteInt();
				vf_discriptor = VirtualFileInst.createVFDiscriptor(level_count);
				 
				for(int i=0;i<this.discriptor_collumn_dim;i++)
				{
					for(int j=0;j<this.level_count;j++)
					{
						this.vf_discriptor[j][i] = io_v.ReadLong() ;
					}
				}
				
				
			} else {
				s_key_id = io_v.ReadLong();
				//s_entry_data[vf_handler_level_indicator] = io_v.read4ByteInt();
				//s_entry_data[vf_handler_id_indicator] = io_v.read4ByteInt();
				//s_entry_data[rec_count_indicator] = io_v.read4ByteInt();
				vf_handler_level = io_v.read4ByteInt();
				vf_handler_id = io_v.ReadLong();
				s_rec_count =  io_v.read4ByteInt();
			}

			// s_position_in_IDKey_store = io_v.ReadLong();
			s_key_string.Read(io_v);
			
			id_count_in_buff = 0;
		}

	}

	public void readData(int[] _ids) throws IOException
	{
		synchronized(this)
		{
			if( id_count_in_buff > 0)
			{
				if(_ids.length < id_count_in_buff)
				{
					System.arraycopy(id_buff, 0, _ids, 0, _ids.length); 
					return;
				}
				else
				{
					System.arraycopy(id_buff, 0, _ids, 0, id_count_in_buff); 
					int remaining_len = _ids.length - id_count_in_buff;
					 
					this.vf_inst.readData(_ids, id_count_in_buff, remaining_len );
					return;
				}
			}
			else
				this.vf_inst.readData(_ids);
			
		}
		
		 
	}
	public void readAndBind(IOInterface io_v, VirtualFileSystem _vfs_binded) throws IOException  {
		
		//long start = System.nanoTime(); 
		
		this.level_count = _vfs_binded.levelCount() + 1;
		 
		Read(io_v);
		if (vf_inst != null)
			vf_inst.close();
		 
		//vf_inst = new VirtualFileInst(_vfs_binded, vf_handler_id, vf_handler_level);
		/*
		vf_inst = new VirtualFileInst(_vfs_binded, 
										vf_handler_id, 
										vf_handler_level,
										vf_discriptor );
		*/
		vf_inst = new VirtualFileInst(_vfs_binded, vf_discriptor );
		
		if(!vf_inst.loadHanlder(vf_handler_id,  vf_handler_level ))
			vf_inst = new VirtualFileInst(_vfs_binded);
		
		//id_buff = new int[200];
		id_count_in_buff = 0;
		//long end = System.nanoTime(); 
		//System.out.println("Binding a vf costs:" + (end-start)+ " ns");
	}

	public void Write(IOInterface io_v) throws IOException {
		synchronized(this)
		{
			if(is_dirty)
			{
				synchronized (io_v) {
					if (io_v.isNative()) {
						this.appendIDBuffToVFS( );
						io_v.WriteLong(s_key_id);
						//((IOStreamNative) io_v).write(s_entry_data, 0, s_entry_data.length);
						io_v.write4ByteInt( vf_handler_level);
						io_v.WriteLong(vf_handler_id);
						io_v.write4ByteInt(s_rec_count);
						for(int i=0;i<this.discriptor_collumn_dim;i++)
						{
							for(int j=0;j<this.level_count;j++)
							{
								io_v.WriteLong(this.vf_discriptor[j][i]);
							}
						}
						
						
						
					} else {
						this.appendIDBuffToVFS( );
						io_v.WriteLong(s_key_id);
						//io_v.write4ByteInt(s_entry_data[vf_handler_level_indicator]);
						//io_v.write4ByteInt(s_entry_data[vf_handler_id_indicator]);
						//io_v.write4ByteInt(s_entry_data[rec_count_indicator]);
						io_v.write4ByteInt( vf_handler_level);
						io_v.WriteLong(vf_handler_id);
						io_v.write4ByteInt(s_rec_count);
						for(int i=0;i<this.discriptor_collumn_dim;i++)
						{
							for(int j=0;j<this.level_count;j++)
							{
								io_v.WriteLong(this.vf_discriptor[j][i]);
							}
						}
						 
					}

					// io_v.WriteLong(s_position_in_IDKey_store);
					s_key_string.Write(io_v);
				}
			}
			is_dirty = false;
		}
	}

	/*
	 * public void setKeyID(int key_id) { entry_data[key_id_indicator] = key_id;
	 * }
	 */
	public long getKeyID() {
		return s_key_id;
	}
	
	public String getKeyString() throws UnsupportedEncodingException
	{
		return this.s_key_string.Get();
	}

	public long getPosition() {
		return this._my_position;
	}

	public int getHandlerLevel() {
		//return s_entry_data[vf_handler_level_indicator];
		return vf_handler_level;
	}

	public void setHandlerLevel(int _handler_level) {
		//s_entry_data[vf_handler_level_indicator] = _handler_level;
		vf_handler_level  = _handler_level;
		is_dirty = true;
	}

	public long getHandlerID() {
		//return s_entry_data[vf_handler_id_indicator];
		return vf_handler_id;
	}

	public void setHandlerID(long _handler_id) {
		//s_entry_data[vf_handler_id_indicator] = _handler_id;
		vf_handler_id = _handler_id;
		is_dirty = true;
	}

	public VirtualFileInst getVFInst() {
		return this.vf_inst;
	}

	public void appendData(int record_id) throws IOException {
		 
		if (this.vf_inst.appendDataGreedy(record_id))
		{
			vf_discriptor = this.vf_inst.getVFDiscriptor();
			setHandlerLevel(vf_inst.getHandler().handler_level);
			setHandlerID(vf_inst.getHandler().handler_id);
			
			this.incrementRecCount(1);
			is_dirty = true;
		} 
	}
	
	public void appendDataAsync(int record_id, StoreRowEntries storage) throws IOException {
		id_buff[id_count_in_buff] = record_id;
		id_count_in_buff ++;
		s_rec_count ++;
		is_dirty = true;  
		
		if( id_count_in_buff >= id_buff.length)
		{
			//appendIDBuffToVFS( ); 
			/*
			 * @STEP 1:
			 * since the handler and vf_discriptor have been updated, 
			 * and the virtual file blocks have been flushed, 
			 * if here does not save and system fails
			 * (because of power off, sys crash or sth),
			 * then when Lunarbase restarts, it still seeks data from 
			 * the old handler and vf_discriptor.
			 */
			//this.Write(storage);
			storage.write(this);
			/*
			 * @STEP 2:
			 * then collect the garbage(the blocks that have their data removed 
			 * to new bigger blocks)
			 */
			this.vf_inst.garbo();
		}

		
	}

	private void appendData(int[] records, int from, int length) throws IOException {
		int end = this.vf_inst.appendDataGreedy(records, from, length);
		if (end >= from)
		{
			vf_discriptor = this.vf_inst.getVFDiscriptor();
			setHandlerLevel(vf_inst.getHandler().handler_level);
			setHandlerID(vf_inst.getHandler().handler_id);
			this.incrementRecCount(end - from + 1);
			is_dirty = true;
		}
	}
	
	private void appendIDBuffToVFS( ) throws IOException {
		int end = this.vf_inst.appendDataGreedy(this.id_buff, 0, this.id_count_in_buff );
		if (end >= 0)
		{
			vf_discriptor = this.vf_inst.getVFDiscriptor();
			setHandlerLevel(vf_inst.getHandler().handler_level);
			setHandlerID(vf_inst.getHandler().handler_id); 
			this.id_count_in_buff = 0;
			
			
			is_dirty = true;
		}
	}

	private void incrementRecCount(int _count) {
		//s_entry_data[rec_count_indicator] += _count;
		s_rec_count += _count;
		is_dirty = true;
	}

	public int getRecCount() {
		//return s_entry_data[rec_count_indicator];
		return s_rec_count;
	}

	public int sizeInBytes() {
		/* 
		 * return 20+8 
		 */
		return 24 + 8*level_count*discriptor_collumn_dim + s_key_string.sizeInBytes();
	}

	public static int getConstantSize(int __level_count) {
		return 24  + 8*__level_count*discriptor_collumn_dim;  

	}

	public boolean isDirt()
	{
		return this.is_dirty;
	}
	@Override
	public void setID(int _id) {
		// TODO Auto-generated method stub

	}

}

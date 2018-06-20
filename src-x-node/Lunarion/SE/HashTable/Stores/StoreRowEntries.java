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

package Lunarion.SE.HashTable.Stores;

import java.io.File;
import java.io.IOException;

import LCG.FSystem.CopyOnWrite.VirtualFileInst;
import LCG.FSystem.CopyOnWrite.VirtualFileSystem;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Manifold.LFSBaseLayer;
import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.IO.L1.IOStream;
import LCG.StorageEngin.IO.L1.IOStreamNative;
import LCG.StorageEngin.Serializable.Impl.String256CharsNew;
import LCG.Utility.StrLegitimate;
import LunarX.Node.Conf.EnginPathDefinition;
import Lunarion.SE.AtomicStructure.RowEntry;

/*
 * StoreRowEntry is binded with a virtual file system, 
 * from where we query the records that link to a specific row entry, 
 * i.e. a key. 
 */ 
public class StoreRowEntries extends LFSBaseLayer {
	private long max_key_id;
	private long used_length_in_latest_ios;
	
	private final String root_path;
	private final String column_name;
	private final VirtualFileSystem _vfs;
	/*
	 * address directory is a file of long integers, each is the address of a
	 * row entry. Find where a row entry i locates is finding the i-th long
	 * integer in this file.
	 */
	// private final IOStreamNative address_dir;
	private final int byte_len_long = 8;
	private final int long_offset = 3; //1<<3 = 8;

	public StoreRowEntries(String _root_path, 
			String _column_name, 
			String store_file_name, 
			String mode, 
			int bufbitlen, 
			DBFSProperties dbfs_prop_instance) throws IOException 
			 {
		// super(_root_path+store_file_name, mode, bufbitlen);
		super(_root_path, store_file_name, mode, bufbitlen, true, dbfs_prop_instance);

		// address_dir = new
		// IOStreamNative(_root_path+EnginPathDefinition.row_entry_address_dir,
		// mode, bufbitlen);
		max_key_id = (objects_dir.length() / byte_len_long) - 1;
		// max_key_id = (int) (this.length()/RowEntry.getStaticSize())-1;

		if (!_root_path.endsWith("/"))
			root_path = StrLegitimate.purifyStringEn(_root_path) + "/";
		else
			root_path = StrLegitimate.purifyStringEn(_root_path);

		column_name = StrLegitimate.purifyStringEn(_column_name);

		String big_table_dir = root_path + DBFSProperties.manifold_dir;
		File big_table_dir_file = new File(big_table_dir);

		if (!big_table_dir_file.exists() && !big_table_dir_file.isDirectory()) {
			big_table_dir_file.mkdir();
		}

		System.out.println("[INFO]: Loading lunar virtual file system for column "+ column_name + "......");
		_vfs = new VirtualFileSystem(big_table_dir, column_name, "rw", bufbitlen, true, dbfs_prop_instance);
		System.out.println("[SUCEED]");
		
		if(max_key_id>=0)
		{
			RowEntry rw = this.get(max_key_id); 
			used_length_in_latest_ios = rw.getPosition()%this.seg_file_length + rw.sizeInBytes();
		}
		else
			used_length_in_latest_ios = 0;
	}

	 
	 
	
	/*
	 * faked for LFSBaseLayser.appendObjInDisk()
	 */
	protected void appendDir()
	{
		
	}

 
	protected long calcDataLengthInLatestIOStream()
	{
		return this.used_length_in_latest_ios;
	};
	

	@Deprecated
	/*
	 * Deprecated because the key_id will never be allowed to be evaluated by 
	 * other storage. 
	 * The only place to know the key_id of a key string is here, the StoreRowEntries.
	 */
	public RowEntry add(long key_id, String str_val) throws IOException {
 
		int size_needed = RowEntry.getConstantSize(_vfs.levelCount()+1) + (new String256CharsNew(str_val)).sizeInBytes();
		 
		if (appendObjInDisk(size_needed) == -1) {
			return null;
		} else {
			this.max_key_id++;
			if (this.max_key_id != key_id) {
				String erroe_info = "[Error]: severe error @StoreRowEntry.add(,) that input key id "
						+ key_id 
						+" is not equal to the actural id it should be: "
						+ this.max_key_id;
				System.err.println(erroe_info);
				throw new IOException(erroe_info);
			}
 
			IOInterface io_latest = this.getLatest();
			/*
			 * the file length minus the size_needed is where the row entry
			 * begins.
			 */
			//long begin_pos = (this.cardinates.size() - 1) * this.seg_file_length + io_latest.dataLength() - size_need;

			//long offset = 0;
			long begin_pos  = 0;
			if((this.used_length_in_latest_ios+size_needed) > this.seg_file_length)
			{
				this.used_length_in_latest_ios = 0;
				begin_pos = (this.cardinates.size() - 1) * this.seg_file_length + 0;

			}
			else
				begin_pos = (this.cardinates.size() - 1) * this.seg_file_length + this.used_length_in_latest_ios;

			synchronized (this.objects_dir) {
				this.objects_dir.seekEnd();
				this.objects_dir.WriteLong(begin_pos);
				//this.objects_dir.flush();
			}
			
		 
			
			RowEntry re = new RowEntry(begin_pos, max_key_id, _vfs, str_val);
			//int index = (int) (begin_pos / this.seg_file_length);
			int offset = (int) (begin_pos % this.seg_file_length);
			int index = this.cardinates.size()-1;
			synchronized (this.cardinates.get(index)) {
				io_latest.seek(offset);
				re.Write(io_latest); 
				
				this.used_length_in_latest_ios += size_needed;
				return re;
			}

		}

	}
	
	public RowEntry add( String str_val) throws IOException {
		 
		int size_needed = RowEntry.getConstantSize(_vfs.levelCount()+1) 
						 + (new String256CharsNew(str_val)).sizeInBytes();
		 
		if (appendObjInDisk(size_needed) == -1) {
			return null;
		} else {
			this.max_key_id++;
			 
			IOInterface io_latest = this.getLatest();
			/*
			 * the file length minus the size_needed is where the row entry
			 * begins.
			 */
			//long begin_pos = (this.cardinates.size() - 1) * this.seg_file_length + io_latest.dataLength() - size_need;

			//long offset = 0;
			long begin_pos  = 0;
			if((this.used_length_in_latest_ios+size_needed) > this.seg_file_length)
			{
				this.used_length_in_latest_ios = 0;
				begin_pos = (this.cardinates.size() - 1) * this.seg_file_length + 0;

			}
			else
				begin_pos = (this.cardinates.size() - 1) * this.seg_file_length + this.used_length_in_latest_ios;

			
			
			RowEntry re = new RowEntry(begin_pos, max_key_id, _vfs, str_val);
			//int index = (int) (begin_pos / this.seg_file_length);
			int offset = (int) (begin_pos % this.seg_file_length);
			int index = this.cardinates.size()-1;
			synchronized (this.cardinates.get(index)) {
				io_latest.seek(offset);
				re.Write(io_latest); 
				
				this.used_length_in_latest_ios += size_needed;
				//return re;
			}
			
			/*
			 * if succeed appending new RowEntry, 
			 * then update the dir to mark the maximum entry id.
			 * 
			 * If system crashes before here, then this new entry will be invisible, 
			 * since the objects_dir has no chance to update.
			 */
			synchronized (this.objects_dir) {
				
				//this.objects_dir.seekEnd();
				//this.objects_dir.WriteLong(begin_pos);
				//this.objects_dir.flush();
				/*
				 * each dir entry is a long value, hence has 8 bytes.
				 */
				this.objects_dir.appendDir(byte_len_long, begin_pos);
			} 
			
			return re;

		}

	}

	/**
	 * Get a row entry from specified key id.
	 * 
	 * @param key_id
	 *            the specified key id
	 * @return if succeed, returns the row entry, otherwise returns null
	 *  
	 * @throws IOException
	 */
 
	public RowEntry get(long key_id)  {

		long position = -1L;

		synchronized (this.objects_dir) {
			if( (this.objects_dir.length() >> long_offset) < key_id)
				return null;
			
			try {
				this.objects_dir.seek(key_id * this.byte_len_long); 
				position = this.objects_dir.ReadLong();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		if (position < 0)
			// TODO
			return null;
		else {
			RowEntry re = new RowEntry(position);
			int index = (int) (position / this.seg_file_length);
			int offset = (int) (position % this.seg_file_length);
			synchronized (this.cardinates.get(index)) {
				IOInterface io_i = this.cardinates.get(index);
				try {
					io_i.seek(offset);
					re.readAndBind(io_i, _vfs);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
				
				return re;
			}
		}
	}

	 
	public void write(RowEntry _re) throws IOException {
		 

		long position = _re.getPosition();

		int index = (int) (position / this.seg_file_length);
		int offset = (int) (position % this.seg_file_length);
		synchronized (this.cardinates.get(index)) {
			IOInterface io_i = this.cardinates.get(index);
			io_i.seek(offset);

			_re.Write(io_i);
			return;
		}

	}

	public void flush() throws IOException {
		super.flush(); 
	}
	
	public void flushVFSCache() throws IOException {
		_vfs.flushCache();
	}
	
	public void flushVFSBaseIO() throws IOException {
		_vfs.flushBaseIO();
	}

	public void close() throws IOException {
		super.close();
		_vfs.close();
	}

}
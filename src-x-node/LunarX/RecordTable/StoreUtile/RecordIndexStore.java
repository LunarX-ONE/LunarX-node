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

package LunarX.RecordTable.StoreUtile;

import java.io.IOException;

import LCG.FSystem.Manifold.TableMeta;
import LCG.StorageEngin.IO.L1.IOStreamNative;
import LunarX.RecordTable.RecStatusUtile.RecStatus;
import Lunarion.SE.AtomicStructure.RecordIndexEntry;

public class RecordIndexStore extends IOStreamNative {
	private final TableMeta index_meta;
	
	/*
	 * stores the maximum used length of record table;
	 */
	private final TableMeta table_meta;
	
	public RecordIndexStore(String table_name, String mode, int bufbitlen) throws IOException {
		// 2^12=4*1024=4K buff size, bigger then faster
		//String store_file_name = TablePath.getTableIndexFile(table_name);
		super(TablePath.getTableIndexFile(table_name), mode, bufbitlen);
		
		index_meta = new TableMeta( TablePath.getTableIndexFile(table_name) + ".meta");

		table_meta = new TableMeta( table_name + ".meta");
		
		if(index_meta.getMaxVal() < 0)
		{
			//index_meta.max_value = 0;
			//table_meta.max_value = 0;
			index_meta.setMaxVal(0);
			table_meta.setMaxVal(0);
		}
		 
	}

	 
	/* 
	public long appendIndexEntry(long position) throws IOException {
		synchronized (this) {
			seekEnd();
			long rec_id = this.length() / RecordIndexEntry.Size() ;
			//RecordIndexEntry rie = new RecordIndexEntry(rec_id, position);
			RecordIndexEntry rie = new RecordIndexEntry(position);
			rie.insert();
			rie.Write(this);
			return rec_id;
		}
	}*/
	 
	
	 
	public long appendIndexEntry(long position) throws IOException {
		synchronized (this) {
			growGreedy(index_meta.getMaxVal() , RecordIndexEntry.SizeInByte()); 
			seek(index_meta.getMaxVal() );
			long rec_id = index_meta.getMaxVal()  / RecordIndexEntry.SizeInByte() ;
			RecordIndexEntry rie = new RecordIndexEntry(position);
			rie.insert();
			rie.Write(this);
			
			//index_meta.max_value = index_meta.max_value + RecordIndexEntry.Size();
			index_meta.setMaxVal(index_meta.getMaxVal() + RecordIndexEntry.SizeInByte());
			return rec_id;
		}
	}
  
	public void flush() throws IOException 
	{ 
		super.flush(); 
		index_meta.updateMeta();
		table_meta.updateMeta();
	}
	
	public void close() 
	{ 
		try {
			index_meta.updateMeta();
			table_meta.updateMeta();
			index_meta.close(); 
			table_meta.close();
		} catch (IOException e) { 
			e.printStackTrace();
			System.err.println("[ERROR]: fail to close @RecordIndexStore.close()");
		}
		finally
		{
			super.close();
		}
		
	}
	
	public long getMaxID() {
		synchronized (this) {
			//return (this.length() / RecordIndexEntry.Size())-1;
			return (index_meta.getMaxVal() /  RecordIndexEntry.SizeInByte()) -1;
		}
	}
	
	public long getMaxUsedLength()
	{
		synchronized (this) {
			return  table_meta.getMaxVal();
		}
	}
	
	public void setMaxUsedLength(long global_position, int rec_length ) throws IOException
	{
		synchronized (this) {
			//table_meta.max_value = global_position + rec_length;
			table_meta.setMaxVal( global_position + rec_length); 
		}
	}

	public RecordIndexEntry getIndexEntry(long rec_id) throws IOException {
		synchronized (this) {
			/*
			 * if rec_id exceeds the maximun id in the table, just return null.
			 * Do not apply seek in native IO library, 
			 * which expand the file length. And if we do so, the next time 
			 * we call getMaxID will be wrong, since the getMaxID calls file file length 
			 * to calculate how many entries in the file. 
			 * 
			 * Is is a bug?
			 */
			if(rec_id > getMaxID() || rec_id <0)
				return null;
			
			seek(rec_id * RecordIndexEntry.SizeInByte());
			RecordIndexEntry rie = new RecordIndexEntry();
			rie.Read(this);
			if(rie.s_rec_status == -1 || rie.s_position == -1)
					/*
					 * do not return null even if deleted,
					 * otherwise, we do not know how many bytes are used in data file
					 * see @RecordHandlerCenter constructor
					 */
					//|| rie.isDeleted())
				return null;
			return rie;
		}
	}
	
	public boolean deleteIndexEntry(long rec_id) throws IOException {
		synchronized (this) {
			if(rec_id > getMaxID() || rec_id <0)
				return false;
			
			seek(rec_id * RecordIndexEntry.SizeInByte());
			RecordIndexEntry rie = new RecordIndexEntry();
			rie.Read(this);
			if(rie.s_rec_status == -1 || rie.s_position == -1
					|| rie.isDeleted())
				return false;
			
			seek(rec_id * RecordIndexEntry.SizeInByte());
			rie.delete();
			rie.Write(this);
			
			return true;
		}
	}
	
	public boolean setIndexEntryStatus(long rec_id, RecStatus _status) throws IOException {
		synchronized (this) {
			if(rec_id > getMaxID() || rec_id <0)
				return false;
			
			seek(rec_id * RecordIndexEntry.SizeInByte());
			RecordIndexEntry rie = new RecordIndexEntry();
			rie.Read(this);
			if(rie.s_rec_status == -1 || rie.s_position == -1
					|| rie.isDeleted())
				return false;
			
			seek(rec_id * RecordIndexEntry.SizeInByte());
			switch(_status)
			{
				case succeed:
					rie.cmdSucceed();
					break;
				case failed:
					rie.cmdFailed();
					break;
				default:
					break;
			} 
			rie.Write(this);
			
			return true;
		}
	}
	
	
	/*
	 * returns the old position
	 */
	//public long updateIndexEntry(long rec_id, long new_addr, int new_version) throws IOException {
	public void updateIndexEntry(long rec_id, RecordIndexEntry rec_index_entry) throws IOException {
			synchronized (this) {
			
			if(rec_id > getMaxID())
				return ;
			
			seek(rec_id * RecordIndexEntry.SizeInByte());
			//RecordIndexEntry rie = new RecordIndexEntry( ) ;
			//rie.Read(this);
			//if(rie.s_rec_status == -1 || rie.s_position == -1
			//		|| rie.isDeleted())
			//	return -1;
			
			//long old_pos = rie.s_position;
			//seek(rec_id * RecordIndexEntry.Size());
			//rie.update(new_addr, new_version); 
			//rie.Write(this);
			//return old_pos;
			rec_index_entry.Write(this);
			
			
		}
	}

}

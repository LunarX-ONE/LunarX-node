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
 

package LunarX.RecordTable;

import java.io.File; 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Manifold.TableMeta;
import LCG.Utility.StrLegitimate;
import LunarX.Memory.TableMemoryStore;
import LunarX.RecordTable.RecStatusUtile.RecStatus;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
import LunarX.RecordTable.StoreUtile.RecordHandler;
import LunarX.RecordTable.StoreUtile.RecordIndexStore;
import LunarX.RecordTable.StoreUtile.TablePath;
import Lunarion.SE.AtomicStructure.RecordIndexEntry;
import Lunarion.SE.AtomicStructure.RowEntry;

public class RecordHandlerCenter {

	private int max_record_id;
	private long used_length_in_latest_ios;
	
	private final RecordIndexStore rec_index;
	private final TableMemoryStore g_records_memory_store; 
	
	private int default_buf_bit_len ;

	private List<RecordHandler> writer_list = new ArrayList<RecordHandler>();
	//private final TableMeta table_meta;
	
	private String table_name;
	private String table_path;
	
	private RecordHandler current_table_writer;
	private int current_index = -1;
	 
	private DBFSProperties dbfs_prop_instance;
	
	public RecordHandlerCenter(File _table_path, 
			TableMemoryStore _memory_store, 
			DBFSProperties _dbfs_prop_instance) throws IOException {
		
		dbfs_prop_instance = _dbfs_prop_instance;
		table_name = _table_path.getAbsolutePath() + "/" + _table_path.getName();
		table_path = _table_path.getAbsolutePath() + "/";
		default_buf_bit_len = dbfs_prop_instance.bit_buff_len;
		rec_index = new RecordIndexStore(table_name, "rw", dbfs_prop_instance.bit_buff_len);
		 
		//table_meta = new TableMeta( table_name + ".meta");
		
		g_records_memory_store = _memory_store;
		
		current_index = 0;
		int max_index = 0;
		while (true) {
			String filename = TablePath.getTableFile(table_name) + "." + max_index;
			File file = new File(filename);
			if (file.exists()) 
			{
				RecordHandler rw = new RecordHandler(
						TablePath.getTableFile(table_name)+"."+max_index,"rw",default_buf_bit_len);
				writer_list.add(rw);
				current_index = max_index;
				max_index++;
			} else
				break;
		}

		if(max_index==0)
		{
			current_table_writer = new RecordHandler(TablePath.getTableFile(table_name) + "." + current_index, "rw",
				default_buf_bit_len);
			writer_list.add(current_table_writer);
		}
		else
			current_table_writer = writer_list.get(current_index);
		
		max_record_id = (int)rec_index.getMaxID(); 
	 
		//while(max_record_id>=0)
		if(max_record_id>=0)
		{/*
			RecordIndexEntry rie = rec_index.getIndexEntry(max_record_id);
			long local_pos = rie.s_position % DBFSProperties.records_table_file_size;
			Record32KBytes rec = current_table_writer.readRecord(max_record_id, local_pos); 
			if(rec!=null)
			{
				used_length_in_latest_ios = local_pos + rec.recLength();
				break;
			}
			else
				max_record_id--;
			*/
			//used_length_in_latest_ios = table_meta.max_value  % dbfs_prop_instance.records_table_file_size;;
			used_length_in_latest_ios = rec_index.getMaxUsedLength()  % dbfs_prop_instance.records_table_file_size;;
			
		}
		 
		if(max_record_id<0)
		{
			used_length_in_latest_ios = 0;
			//table_meta.max_value = 0;
		}
	} 

	
	public String getTablePath()
	{
		return this.table_path;
	}
	
	public DBFSProperties getFSProperties()
	{
		return dbfs_prop_instance;
	}
	/*
	 * returns the global position this record locates.
	 */
	private long internalInsert(Record32KBytes r32kb) throws IOException 
	{
		//if (current_table_writer.length() + r32kb.recLength() > DBFSProperties.records_table_file_size)
		if (used_length_in_latest_ios + r32kb.recLength() > dbfs_prop_instance.records_table_file_size) 
		{
			//if (current_table_writer.length() < DBFSProperties.records_table_file_size) {
			//	current_table_writer.padding(new byte[(int) (DBFSProperties.records_table_file_size - current_table_writer.length())]);
			//}
					
			used_length_in_latest_ios = 0;
			current_index++;
			current_table_writer.flush(); 
			//current_table_writer.close();
			//table_meta.updateMeta();
			rec_index.flush();
			current_table_writer = new RecordHandler(TablePath.getTableFile(table_name) + "." + current_index, "rw",
							default_buf_bit_len);
					
			writer_list.add(current_table_writer);
		}
		current_table_writer.growGreedy(used_length_in_latest_ios, r32kb.recLength());
		//long local_position = current_table_writer.insertRecord(r32kb);
		long local_position = used_length_in_latest_ios;
		current_table_writer.seek(used_length_in_latest_ios);
		current_table_writer.insertRecord(r32kb);
		//long endpos = current_table_writer.GetCurrentPos(); 
				
		long global_pos = local_position +  dbfs_prop_instance.records_table_file_size * current_index;
		//int id_max = rec_index.appendIndexEntry(global_pos);
		//rec_index.appendIndexEntry(global_pos);
				 
		used_length_in_latest_ios += r32kb.recLength();
		
		//table_meta.max_value = global_pos + r32kb.recLength(); 
		rec_index.setMaxUsedLength(global_pos, r32kb.recLength());
		
		this.g_records_memory_store.addToCache(r32kb);
		return global_pos;
	}
	/*
	 * returns this new record
	 */
	public Record32KBytes insertRecord(String rec_str) throws IOException {
		/*
		 * legitimate check, shall be more grammar check!
		 */
		Record32KBytes r32kb = null;
		if(rec_str == null)
		{ 
			return new Record32KBytes(-1);
		}
		if (!rec_str.startsWith("{") || !rec_str.endsWith("}")) {
			 
			return new Record32KBytes(-1);
		}
		
		String purified_rec = StrLegitimate.purifyStringCnEn(rec_str);
		
		int id = (int)rec_index.getMaxID() +1;
		r32kb = new Record32KBytes(id, purified_rec);  
		
		if(r32kb.getID() < 0)
			return r32kb;
		long global_pos = internalInsert(r32kb);
		
		/*
		 * insert succeed, then append index.
		 */
		rec_index.appendIndexEntry(global_pos);
		
		return r32kb;

	}

	public boolean deleteRecord(int rec_id) throws IOException 
	{ 
		return rec_index.deleteIndexEntry(rec_id);
	}

	public boolean setRecStatus(int rec_id, RecStatus _status) throws IOException 
	{ 
		return rec_index.setIndexEntryStatus(rec_id, _status);
	}
	public Record32KBytes updateRecord(Record32KBytes rec) throws IOException
	{ 
		RecordIndexEntry rie = rec_index.getIndexEntry(rec.getID());
		if(rie == null)
			return null;
		if(rie.isDeleted())
			return null;
		
		long old_position = rie.s_position;
		rec.setTrxRollBack(old_position);
		long new_position = internalInsert(rec); 
		
		rie.update(new_position, rec.getVersion()); 
		//rec_index.updateIndexEntry(rec.getID(), new_position, rec.getVersion());
		rec_index.updateIndexEntry(rec.getID(), rie);
		return rec;
		
	}
	
	public RecordHandler getHandler(int i)
	{
		 return writer_list.get(i);
	}
	
	public int recordsCount()
	{
		return (int)rec_index.getMaxID()+1;
	} 
	
	public int getMaxID()
	{
		synchronized(this)
		{
			return (int)rec_index.getMaxID() ;
		} 
	} 
	
	public RecordIndexEntry getIndexEntry(int rec_id) throws IOException
	{
		RecordIndexEntry rie = rec_index.getIndexEntry(rec_id);
		if(rie == null)
			return null;
		if(rie.isDeleted())
			return null;
		
		return rie;
	}
	
	public void flushLatestWriter() throws IOException   
	{
		current_table_writer.flush();
		//table_meta.updateMeta();
		rec_index.flush();
	}
	public void flush() throws IOException {

		for (int i = 0; i < writer_list.size(); i++) {
			writer_list.get(i).flush();
		}

		current_table_writer.flush();
		//table_meta.updateMeta();
		rec_index.flush();
	}

	public void close() throws IOException {

		for (int i = 0; i < writer_list.size(); i++) {
			writer_list.get(i).flush();
			writer_list.get(i).close();
		}
		//table_meta.updateMeta();
		//table_meta.close();
		current_table_writer.close();
		rec_index.close();
	}
}

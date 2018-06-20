package LunarX.Realtime.Column;

import java.io.File; 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import LCG.FSystem.Def.DBFSProperties;
import LCG.MemoryHashTable.LunarMHTNative64;
import LCG.MemoryIndex.LunarMax;
import LCG.MemoryIndex.LunarMaxInterface;
import LunarX.Memory.Closable;
import LunarX.RecordTable.RecordHandlerCenter;
import LunarX.RecordTable.StoreUtile.RecordIndexStore;
import LunarX.RecordTable.StoreUtile.TablePath;
import Lunarion.SE.AtomicStructure.RecordIndexEntry; 

public class StringColumnHandlerCenter extends Closable{

	private long max_lunarmax_record_id;
	private long used_length_in_latest_ios;
	
	private final String rt_column_path;
	private final String column_name;
	
	private final RecordIndexStore lunarmax_rec_index;
	private final RecordHandlerCenter g_record_handler_center; 
	
	private int default_buf_bit_len ;

	//private List<RecordHandler> writer_list = new ArrayList<RecordHandler>();
	private List<String> ios_list = new ArrayList<String>();
	
	//private RecordHandler current_string_table_writer;
	private ValuesStringHandler current_string_column_writer;
	private int current_index = -1;
	 

	public StringColumnHandlerCenter(String _column_name, RecordHandlerCenter _rhc) throws IOException {
		
		column_name = _column_name;
		g_record_handler_center = _rhc;
		
		default_buf_bit_len = _rhc.getFSProperties().bit_buff_len;
		
		
		//rt_table_path = TablePath.getTablePath() + _property_name + "/";
		rt_column_path = _rhc.getTablePath()  + _column_name + "/";
		
		File rttable_path_dir = new File(rt_column_path);

		if (!rttable_path_dir.exists() && !rttable_path_dir.isDirectory()) 
			rttable_path_dir.mkdir(); 
		
		lunarmax_rec_index = new RecordIndexStore(rt_column_path + column_name + ".lunarmax.index", 
													"rw", 
													default_buf_bit_len);
		//g_records_memory_store = _memory_store;
		
		current_index = 0;
		int max_index = 0;
		while (true) {
			String filename = rt_column_path + column_name + "." + max_index;
			File file = new File(filename);
			if (file.exists()) 
			{ 
				//IOStreamNative rw = new IOStreamNative(
				//		filename, "rw", default_buf_bit_len);
				//writer_list.add(rw);
				
				ios_list.add(filename);
				
				current_index = max_index;
				max_index++;
			} else
				break;
		}

		if(max_index==0)
		{ 
			current_string_column_writer = new ValuesStringHandler(rt_column_path + column_name + "." + current_index, "rw",
					 	default_buf_bit_len);
			//writer_list.add(current_integer_writer);
			ios_list.add(rt_column_path + column_name + "." + current_index);
		}
		else
		{ 
			//current_integer_writer = writer_list.get(current_index);
			current_string_column_writer = new ValuesStringHandler(
					ios_list.get(current_index), "rw", default_buf_bit_len);
		}
		
		max_lunarmax_record_id = lunarmax_rec_index.getMaxID();
		
	 
		while(max_lunarmax_record_id>=0)
		{
			RecordIndexEntry rie = lunarmax_rec_index.getIndexEntry(max_lunarmax_record_id);
			long local_pos = rie.s_position % g_record_handler_center.getFSProperties().records_table_file_size;
			ValueString val = current_string_column_writer.readValue( local_pos); 
			if(val!=null)
			{
				used_length_in_latest_ios = local_pos + val.recLength();
				break;
			}
			else
				max_lunarmax_record_id--; 
		}
		if(max_lunarmax_record_id < 0)
			used_length_in_latest_ios = 0;
		 
			
		this.shutdown_called = new AtomicBoolean(false);
		this.critical_reference = new AtomicLong(0);
	} 

	 
	public ValueString insertRecord(String rec_str, int rec_id, int version) throws IOException {
		//int lunarmax_id = rec_index.getMaxID() +1;
		
		if(shutdown_called.get())
			return null;
		
		while(critical_reference.get()>0)
			;

		critical_reference.getAndIncrement();
		
		ValueString val = null;
		 
		
		val = new ValueString(rec_id, rec_str, version); 
		 
		 
		if (used_length_in_latest_ios + val.recLength() >  g_record_handler_center.getFSProperties().records_table_file_size) 
		{ 
			
			used_length_in_latest_ios = 0;
			current_index++;
			current_string_column_writer.flush();
			current_string_column_writer.close();
			
			current_string_column_writer = new ValuesStringHandler(rt_column_path + column_name  + "." + current_index, "rw",
					default_buf_bit_len);
			
			ios_list.add(rt_column_path + column_name  + "." + current_index); 
		}
		current_string_column_writer.growGreedy(used_length_in_latest_ios, val.recLength());
		 
		long local_position = used_length_in_latest_ios;
		current_string_column_writer.seek(used_length_in_latest_ios);
		current_string_column_writer.insertValue(val);
		//long endpos = current_table_writer.GetCurrentPos();
		
		
		long global_pos = local_position +   g_record_handler_center.getFSProperties().records_table_file_size * current_index;
		//int id_max = rec_index.appendIndexEntry(global_pos);
		lunarmax_rec_index.appendIndexEntry(global_pos); 
		
		 
		 
		//current_string_table_writer.flush();
		//lunarmax_rec_index.flush();
		 
		
		used_length_in_latest_ios += val.recLength();
	 
		critical_reference.getAndDecrement();
		 
		return val;

	}

	public ValueString updateRecord(String rec_str, int rec_id, int new_version) throws IOException 
	{
		return insertRecord(rec_str, rec_id, new_version);
			
	}
	
	@Deprecated
	public void loadAllRecords( String property, LunarMax l_m) 
	{  
		LunarMHTNative64 _lunar_mht = new LunarMHTNative64(); 
		
		if(max_lunarmax_record_id <0)
			return;
		
		int current_string_table_index = current_index;
		boolean the_latest = true;
		ValuesStringHandler current_string_table = null;
		while(the_latest)
		{
			try
			{
				current_string_table = new ValuesStringHandler
						(ios_list.get(current_string_table_index), 
						"rw",
						default_buf_bit_len);
						//1<<18);
				the_latest = false;
			}
			catch(IOException e)
			{
				current_string_table_index--;
			}
		}
		
		
		for(long lunarmax_id = max_lunarmax_record_id; lunarmax_id >= 0; lunarmax_id--)
		{
			RecordIndexEntry pair;
			try {
				pair = lunarmax_rec_index.getIndexEntry(lunarmax_id);
			} catch (IOException e1) { 
				e1.printStackTrace();
				continue;
			} 
			
			if(pair == null)
				continue;
			//if(pair.isUpdated() || pair.isDeleted())
				/*
				 * do nothing but continue to handle the next.
				 */
			//	continue;
			
			int index = (int)(pair.s_position /  g_record_handler_center.getFSProperties().records_table_file_size);
			long local_pos = pair.s_position %  g_record_handler_center.getFSProperties().records_table_file_size;
			if(index > current_string_table_index)
				continue;
			
			if(index < current_string_table_index)
			{
				current_string_table.close();
				
				try {
					current_string_table = new ValuesStringHandler
							(ios_list.get(index), 
							"rw",
							default_buf_bit_len);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				current_string_table_index = index;
			}
			
			try { 
				current_string_table.seek(local_pos);
				ValueString val = new ValueString();
				val.Read(current_string_table); 
				
				if(_lunar_mht.search(val.getRecID()) > 0)
				{
					/*
					 * if there is already a version of this rec id, 
					 * then do nothing
					 */
				}
				else
				{
					/*
					 * else if the version >0, then insert it to the hash table.
					 * If the version is 0, then need not to insert it to the hash table,
					 * since this record has no lower version of its value before.
					 * 
					 */
					if(val.getVersion().get()>0)
						_lunar_mht.insert(val.getRecID(), val.getVersion().get());
					
					l_m.insertKey(property, val.recData(), val.getRecID() );
				}
					
				/*
				 * if not deleted
				 */
				//RecordIndexEntry rie = g_record_handler_center.getIndexEntry(val.getRecID());
				//if(null != rie)
				//{
				//	if(!rie.isUpdated())
				//		l_m.insertKey(property, val.recData(), val.getRecID() );
				//	else
				//	{
						/*
						 * else compare the version, if a record was updated, 
						 * there must be a future ValueString 
						 * that carries the latest version of data.
						 */
				//		if(rie.getLatestVersion().intValue() == val.getVersion().intValue())
				//			l_m.insertKey(property, val.recData(), val.getRecID() );
				//	}
				//}
				
			} catch (IOException e) {
				 
				e.printStackTrace();
			} 
		} 
		
		current_string_table.close();
		_lunar_mht.close();
	}
	
	public void bulkLoadAllRecords( String column, LunarMaxInterface l_m) 
	{  
		LunarMHTNative64 _lunar_mht = new LunarMHTNative64(); 
		
		if(max_lunarmax_record_id <0)
			return;
		
		int current_string_table_index = current_index;
		boolean the_latest = true;
		ValuesStringHandler current_string_table = null;
		
		long start_ = System.nanoTime();
		
		while(the_latest)
		{
			try
			{
				current_string_table = new ValuesStringHandler
						(ios_list.get(current_string_table_index), 
						"rw",
						//default_buf_bit_len);
						24);
				the_latest = false;
			}
			catch(IOException e)
			{
				current_string_table_index--;
			}
		}
		
		
		for(long lunarmax_id = max_lunarmax_record_id; lunarmax_id >= 0; lunarmax_id--)
		{
			RecordIndexEntry pair;
			try {
				pair = lunarmax_rec_index.getIndexEntry(lunarmax_id);
			} catch (IOException e1) { 
				e1.printStackTrace();
				continue;
			} 
			
			if(pair == null)
				continue;
			//if(pair.isUpdated() || pair.isDeleted())
				/*
				 * do nothing but continue to handle the next.
				 */
			//	continue;
			
			int index = (int)(pair.s_position /  g_record_handler_center.getFSProperties().records_table_file_size);
			long local_pos = pair.s_position %  g_record_handler_center.getFSProperties().records_table_file_size;
			if(index > current_string_table_index)
				continue;
			
			if(index < current_string_table_index)
			{
				current_string_table.close();
				
				try {
					current_string_table = new ValuesStringHandler
							(ios_list.get(index), 
							"rw",
							//default_buf_bit_len);
							24);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				current_string_table_index = index;
			}
			
			try { 
				current_string_table.seek(local_pos);
				ValueString val = new ValueString();
				val.Read(current_string_table); 
				
				if(_lunar_mht.search(val.getRecID()) > 0)
				{
					/*
					 * if there is already a version of this rec id, 
					 * then do nothing
					 */
				}
				else
				{
					/*
					 * else if the version >0, then insert it to the hash table.
					 * If the version is 0, then need not to insert it to the hash table,
					 * since this record has no lower version of its value before.
					 * 
					 */
					if(val.getVersion().get()>0)
						_lunar_mht.insert(val.getRecID(), val.getVersion().get());
					
					//l_m.insertKey(column, val.recData(), val.getRecID() );
					l_m.bulkloadInsertKey(column, val.recData(), val.getRecID());
				}
					
				/*
				 * if not deleted
				 */
				//RecordIndexEntry rie = g_record_handler_center.getIndexEntry(val.getRecID());
				//if(null != rie)
				//{
				//	if(!rie.isUpdated())
				//		l_m.insertKey(property, val.recData(), val.getRecID() );
				//	else
				//	{
						/*
						 * else compare the version, if a record was updated, 
						 * there must be a future ValueString 
						 * that carries the latest version of data.
						 */
				//		if(rie.getLatestVersion().intValue() == val.getVersion().intValue())
				//			l_m.insertKey(property, val.recData(), val.getRecID() );
				//	}
				//}
				
			} catch (IOException e) {
				 
				e.printStackTrace();
			} 
		} 
		
		l_m.bulkloadCommit(column);
		
		long end_ = System.nanoTime();
		System.out.println("load column \"" + column +  "\" takes(ms):" + (end_ - start_)/1000000);
		
		current_string_table.close();
		_lunar_mht.close();
	}
	
	
	public void flush() throws IOException { 

		if(shutdown_called.get())
			return ;
		
		/*
		 * waiting the running tasks finish.
		 */
		while(critical_reference.get()>0)
			System.out.println("waiting critical task finish. @StringColumnHandlerCenter.flush()");

		critical_reference.getAndIncrement();
		
		current_string_column_writer.flush();
		lunarmax_rec_index.flush();
		
		critical_reference.getAndDecrement();
	}

	public void close() throws IOException {
 
		/*
		 * then no other thread can let this object do a new operation, 
		 * in other words, critical_reference will not increase.
		 */
		if(!shutdown_called.get())
			shutdown_called.set(true);
		
		/*
		 * waiting the running tasks finish.
		 */ 
		while(critical_reference.get()>0)
			System.out.println("waiting critical task finish. @StringColumnHandlerCenter.close()");

		ios_list.clear();
		 
		current_string_column_writer.flushSync();
		lunarmax_rec_index.flushSync();
		 
		System.out.println("column data sync flushed @StringColumnHandlerCenter.close()");

		current_string_column_writer.close();
		lunarmax_rec_index.close();
	}
}

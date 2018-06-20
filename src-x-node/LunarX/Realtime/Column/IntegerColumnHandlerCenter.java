package LunarX.Realtime.Column;

import java.io.File; 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Manifold.TableMeta;
import LCG.MemoryHashTable.LunarMHTNative64;
import LCG.MemoryIndex.LunarMax;
import LCG.MemoryIndex.LunarMaxInterface;
import LCG.StorageEngin.IO.L1.IOStreamNative;
import LunarX.Memory.Closable;
import LunarX.RecordTable.RecordHandlerCenter;
import LunarX.RecordTable.StoreUtile.TablePath;
import Lunarion.SE.AtomicStructure.RecordIndexEntry;

public class IntegerColumnHandlerCenter extends Closable{

	private long max_lunarmax_record_id;
	private long used_length_in_latest_ios;
	
	//private final TableMemoryStore g_records_memory_store; 
	//private final RecordHandlerCenter g_record_handler_center;
	private final TableMeta table_meta;
	
	private final String rt_column_path;
	private final String column_name;
	
	private int default_buf_bit_len  ;

 	private List<String> ios_list = new ArrayList<String>();
	private IOStreamNative current_integer_writer;
	
	private int current_index = -1; 
 
	private final RecordHandlerCenter g_record_handler_center; 
	
	public IntegerColumnHandlerCenter(String _column_name, RecordHandlerCenter _rhc ) throws IOException {
		
		//g_records_memory_store = _memory_store;
		//g_record_handler_center = _rhc;
		column_name = _column_name;
		g_record_handler_center = _rhc;
		default_buf_bit_len = g_record_handler_center.getFSProperties().bit_buff_len; 
		
		//rt_table_path = TablePath.getTablePath() + _property_name + "/";
		rt_column_path = _rhc.getTablePath()  + _column_name + "/";;
		File rttable_path_dir = new File(rt_column_path);

		if (!rttable_path_dir.exists() && !rttable_path_dir.isDirectory()) 
			rttable_path_dir.mkdir();
		
		 
		table_meta = new TableMeta(rt_column_path + column_name + ".lunarmax.meta");
		
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
			current_integer_writer = new IOStreamNative(rt_column_path + column_name + "." + current_index, "rw",
					 	default_buf_bit_len);
			//writer_list.add(current_integer_writer);
			ios_list.add(rt_column_path + column_name + "." + current_index);
		}
		else
		{ 
			//current_integer_writer = writer_list.get(current_index);
			current_integer_writer = new IOStreamNative(
					ios_list.get(current_index), "rw", default_buf_bit_len);
		}
		
		 
		//max_record_id = g_record_handler_center.getMaxID();
		max_lunarmax_record_id = table_meta.getMaxVal();
		
		if(table_meta.getMaxVal() >= 0)
		{
			//RecordIndexEntry rie = rec_index.getIndexEntry(max_record_id);
			ValueInteger ri = new ValueInteger(); 
			

			int id_per_file =  (int)(g_record_handler_center.getFSProperties().records_table_file_size / ri.recLength());
			
			long local_pos =  (table_meta.getMaxVal() % id_per_file)*ri.recLength();
			current_integer_writer.seek(local_pos);
			ri.Read(current_integer_writer);
			used_length_in_latest_ios = local_pos + ri.recLength();
		}
		else
			used_length_in_latest_ios = 0;
		
		this.shutdown_called = new AtomicBoolean(false);
		this.critical_reference = new AtomicLong(0);
	} 

	 
	public ValueInteger insertRecord(int val_as_a_key, int rec_id, int new_version) throws IOException {
		//int id = rec_index.getMaxID() +1;
		//int id = g_record_handler_center.getMaxID()+1;
		
		if(shutdown_called.get())
			return null;
		
		while(critical_reference.get()>0)
			;

		critical_reference.getAndIncrement();
		
		ValueInteger r_int = new ValueInteger(rec_id, val_as_a_key, new_version); 
		 
		//if (current_table_writer.length() + r32kb.recLength() > DBFSProperties.records_table_file_size)
		if (used_length_in_latest_ios + r_int.recLength() > g_record_handler_center.getFSProperties().records_table_file_size) 
		{ 
			used_length_in_latest_ios = 0;
			current_index++;
			current_integer_writer.flush();
			current_integer_writer.close();
			
			current_integer_writer = new IOStreamNative(rt_column_path + column_name  + "." + current_index, "rw",
					default_buf_bit_len);
			
			//writer_list.add(current_integer_writer);
			
			ios_list.add(rt_column_path + column_name  + "." + current_index);
		}
		current_integer_writer.growGreedy(used_length_in_latest_ios, r_int.recLength());
		 
		long local_position = used_length_in_latest_ios;
		current_integer_writer.seek(used_length_in_latest_ios);
		//current_integer_writer.insertRecord(r32kb);
		r_int.Write(current_integer_writer);
		
		//long endpos = current_table_writer.GetCurrentPos();
		
		
		long global_pos = local_position +  g_record_handler_center.getFSProperties().records_table_file_size * current_index;
		//int id_max = rec_index.appendIndexEntry(global_pos);
		//rec_index.appendIndexEntry(global_pos);
	 
		used_length_in_latest_ios += r_int.recLength();
		//table_meta.max_value++;
		table_meta.setMaxVal(table_meta.getMaxVal()+1);
		//table_meta.updateMeta();
		 
		critical_reference.getAndDecrement();
		return r_int;

	}
	
	public ValueInteger updateRecord(int val, int rec_id, int new_version) throws IOException 
	{
		return insertRecord( val, rec_id, new_version) ;
	}
		
	/*
	 * load data with random value from storage. 
	 * Insert directly into index has terrible low efficiency.
	 * The better solution is to sort the data first, then insert as
	 * ordered set. 10 times faster.
	 * 
	 * Alternative function is @bulkLoadAllRecords
	 */
	@Deprecated
	public void loadAllRecords( String column, LunarMax l_m) 
	{
		LunarMHTNative64 _lunar_mht = new LunarMHTNative64(); 
		
		ValueInteger r_int_reader = new ValueInteger( ); 
		if(table_meta.getMaxVal() <0)
			return;
		
		int id_per_file =  (int)(g_record_handler_center.getFSProperties().records_table_file_size / r_int_reader.recLength());
		
		//long end_pos = table_meta.max_lunarmax_record_id * r_int_reader.recLength()% DBFSProperties.records_table_file_size;
		long end_pos = (table_meta.getMaxVal() % id_per_file )*r_int_reader.recLength();
		
		/*
		 * these ids are all lunarmax persistent id
		 */
		long begin_id = table_meta.getMaxVal() - (table_meta.getMaxVal() % id_per_file) ;
		long end_id = table_meta.getMaxVal();
		long prev_seg_file_end_id = begin_id-1;
		
		long start_ = System.nanoTime();
		
		for(int i=ios_list.size()-1;i>=0;i--)
		{ 
			
			IOStreamNative ios;
			try {
				ios = new IOStreamNative
						(ios_list.get(i), "rw", 
							//default_buf_bit_len); 
								24);
				 
				synchronized(ios) 
				{ 
					while(begin_id <= end_id)
					{
						long local_pos = (end_id% id_per_file)*r_int_reader.recLength();
						try {
							 
							//long start_each = System.nanoTime();
							
							ios.seek(local_pos);
							//r_int_reader.setRecID(begin_id); 
							r_int_reader.Read(ios); 
							
							//long end_each = System.nanoTime();
							//System.out.println("each int_rec read takes(ns):" + (end_each - start_each));
							 
							
							if(_lunar_mht.search(r_int_reader.getRecID()) > 0)
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
								if(r_int_reader.getVersion().get()>0)
									_lunar_mht.insert(r_int_reader.getRecID(), r_int_reader.getVersion().get());
								
								l_m.insertKey(column, r_int_reader.recData(), r_int_reader.getRecID() );
							}
							
							end_id--;
							/*
							 * if not deleted.
							 */
							//RecordIndexEntry rie = g_record_handler_center.getIndexEntry(r_int_reader.getRecID());
							//if(null != rie)
							//{ 	
							//	if(!rie.isUpdated()) 
							//		l_m.insertKey(property , r_int_reader.recData(), r_int_reader.getRecID() );
							  
								/*
								 * else compare the version, if a record was updated, 
								 * there must be a future ValueInteger 
								 * that carries the latest version of data.
								 */
							//	else
							//	{
							//		if(rie.getLatestVersion().intValue() == r_int_reader.getVersion().intValue())
							//			l_m.insertKey(property, r_int_reader.recData(), r_int_reader.getRecID() );
							//	}
								
							//	end_id--;
							//}
							//else
							//	end_id--;
						}
						catch(IOException e) { 
							/*
							 * if this record fails, read the next one
							 */ 
							end_id--;
							
							e.printStackTrace();
						} 
					} 
					ios.close(); 
					  
				}
			 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				/*
				 * if fails, open the next
				 */
				
				e.printStackTrace();
			} 
			
			begin_id = prev_seg_file_end_id - id_per_file +1;
			end_id = prev_seg_file_end_id ; 
			
			prev_seg_file_end_id = begin_id-1;
		}
		
		long end_ = System.nanoTime();
		System.out.println("load column " + column +  " take(ms):" + (end_ - start_)/1000000);
		
		_lunar_mht.close(); 
	}

	
	public void bulkLoadAllRecords( String column, LunarMaxInterface l_m) 
	{
		LunarMHTNative64 _lunar_mht = new LunarMHTNative64(); 
		
		
		ValueInteger r_int_reader = new ValueInteger( ); 
		
		if(table_meta.getMaxVal() <0)
			return;
		
		int id_per_file =  (int)(g_record_handler_center.getFSProperties().records_table_file_size / r_int_reader.recLength());
		
		//long end_pos = table_meta.max_lunarmax_record_id * r_int_reader.recLength()% DBFSProperties.records_table_file_size;
		long end_pos = (table_meta.getMaxVal() % id_per_file )*r_int_reader.recLength();
		
		/*
		 * these ids are all lunarmax persistent id
		 */
		long begin_id = table_meta.getMaxVal() - (table_meta.getMaxVal() % id_per_file) ;
		long end_id = table_meta.getMaxVal();
		long prev_seg_file_end_id = begin_id-1;
		
		long start_ = System.nanoTime();
		
		for(int i=ios_list.size()-1;i>=0;i--)
		{ 
			
			IOStreamNative ios;
			try {
				ios = new IOStreamNative
						(ios_list.get(i), "rw", 
							//default_buf_bit_len); 
								24);
				
				synchronized(ios) 
				{ 
					while(begin_id <= end_id)
					{
						long local_pos = (end_id% id_per_file)*r_int_reader.recLength();
						try {
							 
							//long start_each = System.nanoTime();
							
							ios.seek(local_pos);
							//r_int_reader.setRecID(begin_id); 
							r_int_reader.Read(ios); 
							
							//long end_each = System.nanoTime();
							//System.out.println("each int_rec read takes(ns):" + (end_each - start_each));
							 
							
							if(_lunar_mht.search(r_int_reader.getRecID()) > 0)
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
								if(r_int_reader.getVersion().get()>0)
									_lunar_mht.insert(r_int_reader.getRecID(), r_int_reader.getVersion().get());
								
								//l_m.insertKey(property, r_int_reader.recData(), r_int_reader.getRecID() );
								l_m.bulkloadInsertKey(column, r_int_reader.recData(), r_int_reader.getRecID());
								 
							}
							
							end_id--;
							/*
							 * if not deleted.
							 */
							//RecordIndexEntry rie = g_record_handler_center.getIndexEntry(r_int_reader.getRecID());
							//if(null != rie)
							//{ 	
							//	if(!rie.isUpdated()) 
							//		l_m.insertKey(property , r_int_reader.recData(), r_int_reader.getRecID() );
							  
								/*
								 * else compare the version, if a record was updated, 
								 * there must be a future ValueInteger 
								 * that carries the latest version of data.
								 */
							//	else
							//	{
							//		if(rie.getLatestVersion().intValue() == r_int_reader.getVersion().intValue())
							//			l_m.insertKey(property, r_int_reader.recData(), r_int_reader.getRecID() );
							//	}
								
							//	end_id--;
							//}
							//else
							//	end_id--;
						}
						catch(IOException e) { 
							/*
							 * if this record fails, read the next one
							 */ 
							end_id--;
							
							e.printStackTrace();
						} 
					} 
					ios.close(); 
					
				}
			 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				/*
				 * if fails, open the next
				 */
				
				e.printStackTrace();
			} 
			
			begin_id = prev_seg_file_end_id - id_per_file +1;
			end_id = prev_seg_file_end_id ; 
			
			prev_seg_file_end_id = begin_id-1;
		}
		
		 
		l_m.bulkloadCommit(column);
		
		long end_ = System.nanoTime();
		System.out.println("load column " + column +  " takes(ms):" + (end_ - start_)/1000000);
		
		_lunar_mht.close(); 
	}

	public void flush() throws IOException
	{
		if(shutdown_called.get())
			return ;
		
		/*
		 * waiting the running tasks finish.
		 */
		while(critical_reference.get()>0)
			System.out.println("waiting critical task finish. @IntegerTableHandlerCenter.flush()");

		critical_reference.getAndIncrement();
		
		table_meta.updateMeta();
		current_integer_writer.flush();
		
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
			System.out.println("waiting critical task finish. @IntegerTableHandlerCenter.close()");

		
		ios_list.clear();
		table_meta.updateMeta();
		table_meta.close();
		current_integer_writer.flush();
		current_integer_writer.close();
		 
	}
}

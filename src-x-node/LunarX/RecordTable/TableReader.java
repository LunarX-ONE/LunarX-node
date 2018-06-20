package LunarX.RecordTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import LunarX.Memory.TableMemoryStore;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
import Lunarion.SE.AtomicStructure.RecordIndexEntry;  

public class TableReader {

	public final RecordReaderContainer reader_container;
	public final RecordHandlerCenter handler_center; 
	 
	private final TableMemoryStore g_records_memory_store; 
	
	public RecordIndexEntry pair;  
	  
	public TableReader(TableMemoryStore _memory_store, RecordHandlerCenter _handler_center) throws IOException {
		//store_index = new RecordIndexStore(TablePath.getTableIndexFile(), "r", DBFSProperties.bit_buff_len);
		g_records_memory_store = _memory_store;
		handler_center =  _handler_center;
		
		reader_container = new RecordReaderContainer(_handler_center);
	} 
	 

	public ArrayList<Record32KBytes> getRecordsByIds(Set<Integer> ids, 
													int latest_count) throws IOException {

		//long start_time = System.nanoTime();
		ArrayList<Record32KBytes> arraylist = new ArrayList<Record32KBytes>();
		int count = 0;
		int total = latest_count<=0?ids.size():latest_count;
		Iterator<Integer> id_iter = ids.iterator();
		
		while(id_iter.hasNext() && count<total)
		{
			Record32KBytes rec =  getRecord(id_iter.next());
			if(rec != null)
			{
				arraylist.add(rec);
				count++;
			}
		} 
		 
	 
		return arraylist;
	} 
	
	public ArrayList<Record32KBytes> getRecordsByIds(Set<Integer> ids, 
													int __from, 
													int __count) throws IOException {

	 
		ArrayList<Record32KBytes> arraylist = new ArrayList<Record32KBytes>();
		int count = 0;
		int total = __count<=0?ids.size():__count;
		
		Iterator<Integer> id_iter = ids.iterator();
		 
		while(id_iter.hasNext() && count<total)
		{
			//Record32KBytes rec =  getRecord(ids.next());
			int id = id_iter.next();

			if(count >= __from)
			{
				Record32KBytes rec =  getRecord(id);
				if(rec!=null)
					arraylist.add(rec);
			} 
			count++;  
		}  

		return arraylist;
} 
	
	public ArrayList<Record32KBytes> getRecordsByIds(int ids[]) throws IOException {

		//long start_time = System.nanoTime();
		ArrayList<Record32KBytes> arraylist = new ArrayList<Record32KBytes>();
		 
		for (int i = ids.length - 1; i >= 0; i--) {
		//for (int i = 0; i <= ids.length - 1; i++) {
			//arraylist.add(ids[i] + ": " + getRecord(ids[i]));
			Record32KBytes rec =  getRecord(ids[i]);
			if(rec != null)
				arraylist.add(rec);
		}

		//long end_time = System.nanoTime();
		//System.out.print("feteching records takes time(ns):");
		//System.out.println (end_time - start_time)  ;
		return arraylist;
	} 
	
	public ArrayList<Record32KBytes> getRecordsByIds(int ids[], int latest_count) throws IOException {

		//long start_time = System.nanoTime();
		ArrayList<Record32KBytes> arraylist = new ArrayList<Record32KBytes>();
		
		int record_count = Math.min(ids.length, latest_count);
		
		/*
		 * ascending sort
		 */
		Arrays.sort(ids);
		for (int i = ids.length - 1; i >= (ids.length-record_count); i--) {
			//arraylist.add(ids[i] + ": " + getRecord(ids[i]));
			Record32KBytes rec =  getRecord(ids[i]);
			if(rec != null)
				arraylist.add(rec);
		}

		//long end_time = System.nanoTime();
		//System.out.print("fetech records takes time(ns):");
		//System.out.println (end_time - start_time)  ;
		return arraylist;
	} 
	
	public ArrayList<Record32KBytes> getRecordsByIds(int ids[], int from, int count) throws IOException {

		//long start_time = System.nanoTime();
		ArrayList<Record32KBytes> arraylist = new ArrayList<Record32KBytes>();
		
		from = from<0?0:from;
		int record_count =  count ;
		if(from+count >= ids.length)
			record_count = ids.length -from;
		
		int begin_id = ids.length-1-from;
		int end_id = begin_id - record_count+1;
		for (int i = begin_id; i >= end_id; i--) {
			 
			Record32KBytes rec =  getRecord(ids[i]);
			if(rec != null)
				arraylist.add(rec);
		}
		/*
		for (int i = from + record_count - 1; i >= from; i--) {
			 
			Record32KBytes rec =  getRecord(ids[i]);
			if(rec != null)
				arraylist.add(rec);
		}
		 */
	 
		return arraylist;
	} 
	
	public ArrayList<Record32KBytes> getRecords (int from, int count) throws IOException {

		 
		ArrayList<Record32KBytes> arraylist = new ArrayList<Record32KBytes>();
		 
		int len = Math.min(count, recordsCount());
		int from__ = from<0? 0:from;
		
		int max_id =  recordsCount()-1;
		int begin_id = (max_id - from__);
		if(begin_id<0)
			return arraylist;
		
		int end_id = begin_id -len + 1;
		if(end_id<0)
			end_id = 0;
		
		for (int i = begin_id; i >= end_id; i--) 
		{
			Record32KBytes rec =  getRecord(i);
			if(rec != null)
				arraylist.add(rec);
		}
		 

		/*
		for (int i = from; i < from+count; i++) {
			Record32KBytes rec =  getRecord(i);
			if(rec != null)
				arraylist.add(rec);
		} */
		return arraylist;
	} 
	/*
	 * ASC, i.e. the oldest on the top of the result list.
	 * Example: table has 1000 records, and fetchRecords(0, 100) returns:
	 * rec 0, rec 1, rec 2, ... , rec 99
	 */
	public ArrayList<Record32KBytes> getRecordsASC (int from, int count) throws IOException {

		 
		ArrayList<Record32KBytes> arraylist = new ArrayList<Record32KBytes>();
	 
 
		for (int i = from; i < from+count; i++) {
			Record32KBytes rec =  getRecord(i);
			if(rec != null)
				arraylist.add(rec);
		}  
		return arraylist;
	} 
	
	
	public ArrayList<Record32KBytes> getLatestRecords(int latest_n)
	{
		long start_time = System.nanoTime();
		ArrayList<Record32KBytes> arraylist = new ArrayList<Record32KBytes>();
		 
		int len = Math.min(latest_n, recordsCount());
		  
		int max_id =  recordsCount()-1;
		int index = 0;
		int added = 0;
		while(index < len)
		{
			int ith = max_id-len+index+1;
			Record32KBytes rec =  getRecord(ith);
			if(rec != null)
			{
				arraylist.add(rec);  
				added++;
			} 
			index++;
		} 

		long end_time = System.nanoTime();
		System.out.println("feteching the latest " +added+ " records takes time(ns):" + (end_time - start_time));
		 
		return arraylist;
	}

	public int recordsCount()
	{
		return handler_center.recordsCount();
	}
	
	public Record32KBytes getRecord(int id) {
		try { 
			if (id < 0)
				return null;
			Record32KBytes record = this.g_records_memory_store.getObj(id);
			if(record!=null)
				return record; 
			 
			
			pair = this.handler_center.getIndexEntry(id);
			if(pair == null)
				return null;
			 
			record = reader_container.readRecord(id, pair.s_position);
			if(record!=null)
				//this.g_records_memory_store.addToCache(id, record.recData());
				this.g_records_memory_store.addToCache(record);
			
			return record ;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	} 
	 
}

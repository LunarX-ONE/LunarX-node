package LunarX.Realtime.Column;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

 
import LCG.MemoryIndex.IndexTypes.DataTypes;
import LunarX.RecordTable.RecordHandlerCenter;
import LCG.FSystem.Def.DBFSProperties;
import LCG.MemoryIndex.LunarMax;
import LCG.MemoryIndex.LunarMaxInterface;
import LCG.MemoryIndex.LunarMaxWithStorage;
import LCG.MemoryIndex.IndexTypes;

public class LunarMaxPersistent { 

	HashMap<String, DataTypes> column_map;
	HashMap<String, IntegerColumnHandlerCenter> integer_column_map;  
	//HashMap<String, LongColumnHandlerCenter> long_column_map;   
	HashMap<String, StringColumnHandlerCenter> string_column_map;  
	
	//LunarMax lunar_max = null;
	LunarMaxWithStorage lunar_max = null;
 
	final RecordHandlerCenter record_handler;
	 
	public LunarMaxPersistent(boolean virtual_mem_enabled, RecordHandlerCenter _rhc )
	{
		System.out.println("LunarMax persistent module is loading now......");

		record_handler = _rhc;
		
		column_map = new HashMap<String, DataTypes>();
		integer_column_map = new HashMap<String, IntegerColumnHandlerCenter>();
		//long_column_map = new HashMap<String, LongColumnHandlerCenter>();
		string_column_map = new HashMap<String, StringColumnHandlerCenter>();
		
		if(!virtual_mem_enabled)
		{			
			/*
			lunar_max = new LunarMax(record_handler.getFSProperties().rt_index_order, 
					record_handler.getFSProperties().rt_precision);
					*/
		} 						
		else
		{
			/*
			File swap_dir = new File(record_handler.getFSProperties().rt_vm_swap);

			if (!swap_dir.exists() && !swap_dir.isDirectory()) 
				swap_dir.mkdir();
				
			lunar_max = new LunarMax(record_handler.getFSProperties().rt_virtual_mem_enabled, 
					record_handler.getFSProperties().rt_precision, 
					record_handler.getFSProperties().rt_vm_swap, 
					record_handler.getFSProperties().rt_max_memory, 
					record_handler.getFSProperties().rt_max_virtual_pte, 
					25) ;
			*/
			File swap_dir = new File(record_handler.getTablePath());

			if (!swap_dir.exists() && !swap_dir.isDirectory()) 
				swap_dir.mkdir();
				
			lunar_max = new LunarMaxWithStorage(record_handler.getFSProperties().rt_virtual_mem_enabled, 
					record_handler.getFSProperties().rt_precision, 
					record_handler.getTablePath(), 
					record_handler.getFSProperties().rt_max_memory, 
					record_handler.getFSProperties().rt_max_virtual_pte, 
					12,
					record_handler.getFSProperties().rt_threads) ;
			
		}
		System.out.println("LunarMax persistent module loaded.");

	} 
	
	/*
	 * in case different table has same column names, 
	 * using name_space (normally a table name) to differentiate them.
	 */
	public boolean registerPersisitent(String name_space, String column, DataTypes  data_type) throws IOException
	{ 
		lunar_max.registerIndexer(name_space.trim(), column.trim(), data_type); 
		
		if(column_map.get(column.trim())!=null)
		{
			System.out.println("LunarMax persistent module warning: column \"" + column + "\" already has an indexer.");
			return false;
		}
		
		column_map.put(column.trim(), data_type);
		
		switch(data_type)
		{
		case STRING:
			
			if(string_column_map.get(column.trim())!=null)
				return false;
			
			StringColumnHandlerCenter s_t_h_c = new StringColumnHandlerCenter(column.trim(), record_handler);
			string_column_map.put(column.trim(), s_t_h_c); 
			break; 
		case INTEGER:
			if(integer_column_map.get(column.trim())!=null)
				return false;
			
			IntegerColumnHandlerCenter i_t_h_c = new IntegerColumnHandlerCenter(column.trim(), record_handler);
			integer_column_map.put(column.trim(), i_t_h_c); 
			break;
		case LONG:
			/*
			if(long_column_map.get(column.trim())!=null)
				return false;
			
			LongColumnHandlerCenter l_t_h_c = new LongColumnHandlerCenter(column.trim(), record_handler);
			long_column_map.put(column.trim(), l_t_h_c);  
			*/
			break;
		default:
			return false;
		}
		
		return true;
	}
	
	 
 
	public void loadRecords(String column)
	{
		if(column_map.get(column.trim()) == null)
		{
			return ;
		}
		else
		{
			switch(column_map.get(column.trim()))
			{
			case STRING: 
				string_column_map.get(column.trim()).bulkLoadAllRecords( column.trim(), lunar_max);  
				break; 
			case INTEGER: 
				integer_column_map.get(column.trim()).bulkLoadAllRecords( column.trim(), lunar_max); 
				 
				break;
			case LONG:
				/*
				long_column_map.get(column.trim()).bulkLoadAllRecords( column.trim(), lunar_max); 
				 */
				break;
			}
			return ;
		}
	}
	
	public boolean insertRecord(String column, String value, int rec_id, int version) throws IOException
	{ 
		lunar_max.insertKey(column.trim(), value.trim(), rec_id);
		 
		if(column_map.get(column.trim()) == null)
		{
			return false;
		} 
		 
		else
		{
			switch(column_map.get(column.trim()))
			{
			case STRING: 
				string_column_map.get(column.trim()).insertRecord( value.trim() , rec_id, version);  
				break; 
			case INTEGER:
				integer_column_map.get(column.trim()).insertRecord(Integer.parseInt(value), rec_id, version); 
				break;
			case LONG: 
				/*
				long_column_map.get(column.trim()).insertRecord(Long.parseLong(value), rec_id, version); 
				 */
				break;
			default:
				return false;
			}
			return true;
		}
		
	} 
	
	public boolean deleteRecord(String column, String col_value, int rec_id) 
	{ 
		//return lunar_max.delete(column, col_value, rec_id);
		return false;
	} 
	
	public boolean updateRecord(String column, 
								String column_value, 
								String old_value,
								int rec_id, int new_version) throws IOException 
	{ 
		if(old_value!=null)
			lunar_max.delete(column.trim(), old_value.trim(), rec_id);
		
		if(column_map.get(column.trim()) == null)
		{
			return false;
		} 
		else
		{
			switch(column_map.get(column.trim()))
			{
			case STRING: 
				string_column_map.get(column.trim()).updateRecord( 
								column_value.trim() , rec_id, new_version);  
				break; 
			case INTEGER:
				integer_column_map.get(column.trim()).updateRecord(
								Integer.parseInt(column_value), 
								rec_id, new_version); 
				break;
			case LONG: 
				/*
				long_column_map.get(column.trim()).updateRecord(
						Long.parseLong(column_value), 
						rec_id, new_version);
						*/  
				break;
			}
			
			lunar_max.insertKey(column.trim(), column_value.trim(), rec_id);
			
			return true;
		} 
	} 
	
	public LunarMaxInterface getLunarMax()
	{
		return this.lunar_max;
	}

	public void save()  
	{
		lunar_max.save();
		
		Iterator<String> it0 = string_column_map.keySet().iterator();
		while(it0.hasNext())
		{
			String key = it0.next(); 
			StringColumnHandlerCenter t_sthc = string_column_map.get(key);
			try {
				t_sthc.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		
		Iterator<String> it1 = integer_column_map.keySet().iterator();
		while(it1.hasNext())
		{
			String key = it1.next(); 
			IntegerColumnHandlerCenter t_ithc = integer_column_map.get(key);
			try {
				t_ithc.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		/*
		Iterator<String> it2 = long_column_map.keySet().iterator();
		while(it2.hasNext())
		{
			String key = it2.next(); 
			IntegerColumnHandlerCenter t_lthc = long_column_map.get(key);
			try {
				t_lthc.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		*/
	}
	
	public void close() throws IOException
	{
		System.out.println("LunarMax persistent module is closing now......");
	 
		if (lunar_max != null)
		{
			lunar_max.shutDown();
			lunar_max = null;
		}
		
		Iterator<String> it0 = string_column_map.keySet().iterator();
		while(it0.hasNext())
		{
			String key = it0.next(); 
			StringColumnHandlerCenter t_ithc = string_column_map.get(key);
			t_ithc.close();
		}
		string_column_map.clear();  
		
		Iterator<String> it1 = integer_column_map.keySet().iterator();
		while(it1.hasNext())
		{
			String key = it1.next(); 
			IntegerColumnHandlerCenter t_ithc = integer_column_map.get(key);
			t_ithc.close();
		}
		integer_column_map.clear();  
		
		/*
		Iterator<String> it2 = long_column_map.keySet().iterator();
		while(it2.hasNext())
		{
			String key = it2.next(); 
			LongColumnHandlerCenter t_lthc = long_column_map.get(key);
			t_lthc.close();
		}
		long_column_map.clear();  
		*/
		
		System.out.println("LunarMax persistent module closed.");
		 
	}

}

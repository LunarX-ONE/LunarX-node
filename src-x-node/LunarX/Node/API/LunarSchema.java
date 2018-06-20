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
 

package LunarX.Node.API;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import LCG.FSystem.Def.DBFSProperties;
import LCG.MemoryIndex.IndexTypes.DataTypes;
import LCG.MemoryIndex.LunarMax;
import LCG.Utility.StrLegitimate;
import LunarX.Memory.TableMemoryStore;
import LunarX.Node.API.XNodeStatus.DBRuntimeStatus;
import LunarX.Node.API.Result.FTQueryResult;
import LunarX.Node.API.Result.RGQueryResult;
import LunarX.Node.Conf.EnginPathDefinition;
import LunarX.Node.ThreadTasks.SETaskColumnIndexer;
import LunarX.Node.ThreadTasks.SETaskSearchIDs;
import LunarX.Node.Utils.QuickSort;
import LunarX.Realtime.Column.IntegerColumnHandlerCenter;
import LunarX.Realtime.Column.LunarMaxPersistent;
import LunarX.Realtime.Column.StringColumnHandlerCenter;
import LunarX.RecordTable.RecordHandlerCenter;
import LunarX.RecordTable.TableReader;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
import LunarX.RecordTable.StoreUtile.TablePath;

/*
 * LunarSchema presents a structured database, 
 * with fixed length of columns(fields).
 * Current version supports the following structured data:
 * int(4 bytes), long(8 bytes), char(1 byte), byte(1 byte), 
 * time(8 bytes, presented by a long, yyyy/mm/dd/hh/mm/ss),
 * varchar(m)(m chars for this field),
 * 
 *     
 */
public class LunarSchema extends LunarXNode{
	
	 
	 
	public LunarSchema() {
		super();
	}

	public boolean createSchema(String schema_def) {
		
		 return false;
	}
	
  
	 
	public boolean createTableStructure(String table_name)
	{ 
		File _table_dir = new File(table_path_builder.getTablePath() + table_name);
		 
		return makeDir(_table_dir);
	}
	
	/*
	public boolean vnodeCreateTable(String table_name, int virtual_node_id)
	{
		if(this.virtual_node_map.get(virtual_node_id) == null)
		{
			System.err.println("[ERROR]: virtual node " + virtual_node_id + " does not exist.");
			System.err.println("[TRY]: please create virtual node first and try create table again.");
			return false;
		}
		File _table_dir = new File(TablePath.getVNodeTablePath() 
									+ virtual_node_id
									+ "/" + table_name);
		 
		return makeDir(_table_dir);
	}
	
	public boolean vnodeCreate(int virtual_node_id)
	{
		File _table_dir = new File()
		return 
	}
	
	*/
 
	
	public boolean openTable(String table_name) 
	{ 

		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return false;
		
		if(shutdown_called.get())
			return false;
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_in_writing.get()>0)
			;

		critical_in_writing.getAndIncrement();  
		this.current_status = DBRuntimeStatus.onWriting; 
		
		try
		{
			File _table_dir = new File(table_path_builder.getTablePath() + table_name);
			
			if(_table_dir.isDirectory())
			{ 
				LunarTable one_table = new LunarTable();
				one_table.openTable(_table_dir.getAbsolutePath(), 
						dbfs_prop_instance.real_time_mode, 
						dbfs_prop_instance.max_records_in_memory,
						dbfs_prop_instance);
				this.table_map.put(_table_dir.getName(), one_table);
				return true; 
			}
			return false;
		}
		finally
		{
			/*
			 * the final segment may be interrupted by Ctrl+C, 
			 * when user closes process on cmd console.
			 * So here use the atomic critical_reference to 
			 * tell shutDown()(or other functions which may interrupt this) 
			 * waiting for a moment.
			 */
			this.current_status = DBRuntimeStatus.onWaiting;
			critical_in_writing.getAndDecrement();
			
		}
		
	}
	
	public boolean removeTable(String table_name)
	{
		LunarTable table = table_map.get(table_name);
		if(table == null)
			return false;
		if(table.getStatus() == DBRuntimeStatus.removed)
		{
			System.err.println("[ERROR]: table "+ table_name +" has already been removed @LunarDB.removeTable(), can not be removed again.");
			return false;
		}
		try {
			table.closeTable();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return table.removeTable( );
	}
	
	public boolean restoreTable(String table_name)
	{
		LunarTable table = table_map.get(table_name);
		if(table == null)
			return false;
		
		if(table.getStatus() != DBRuntimeStatus.removed)
		{
			System.err.println("[ERROR]: table "+ table_name +" is in use @LunarDB.restoreTable(), can not be restored.");
			
			return false;
		}
		try {
			table.closeTable();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		if(table.restoreTable( ))
		{
			try {
				table.closeTable();
				
				table.openTable(table.table_root_path, 
						dbfs_prop_instance.real_time_mode, 
						dbfs_prop_instance.max_records_in_memory,
						dbfs_prop_instance);
				
				return true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		else
			return false;
		
	}
	
	public boolean openMaterializedView(String view_name) throws IOException
	{ 
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return false;
		
		if(shutdown_called.get())
			return false;
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_in_writing.get()>0)
			;

		critical_in_writing.getAndIncrement();  
		this.current_status = DBRuntimeStatus.onWriting; 
	try
	{
		File _view_dir = new File(table_path_builder.getViewPath() + view_name);
		
		if(_view_dir.isDirectory())
		{ 
			LunarMView one_view =new LunarMView();
			one_view.openView(_view_dir.getAbsolutePath(), 
					dbfs_prop_instance.real_time_mode, 
					dbfs_prop_instance.max_records_in_memory,
					dbfs_prop_instance);
			this.view_map.put(_view_dir.getName(), one_view);
			return true; 
		}
		return false;
	}
		finally
		{
			/*
			 * the final segment may be interrupted by Ctrl+C, 
			 * when user closes process on cmd console.
			 * So here use the atomic critical_reference to 
			 * tell shutDown()(or other functions which may interrupt this) 
			 * waiting for a moment.
			 */
			this.current_status = DBRuntimeStatus.onWaiting;
			critical_in_writing.getAndDecrement();
			
		}
	}
	
	public LunarTable getTable(String table)
	{
		return this.table_map.get(table);
	}
	
	public LunarMView getMView(String view)
	{
		return this.view_map.get(view);
	}
	
	public Iterator<String> listTable()
	{
		return table_map.keySet().iterator();
	}
	
	public DBRuntimeStatus tableStatus(String table)
	{
		LunarTable t = table_map.get(table);
		if(t != null)
			return t.getStatus();
		else
			return null;
	}
	
	public boolean openDB(String _root_path)  {
		return false;
	}

	 
	 
	/*
	 * JSON records like: {name=jackson, payment=500, age=30}
	 */
	public Record32KBytes insertRecord(String table, String record) throws InterruptedException, ExecutionException 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return null;
		
		if(shutdown_called.get())
			return null;
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_in_writing.get()>0)
			;

		critical_in_writing.getAndIncrement();  
		this.current_status = DBRuntimeStatus.onWriting; 
		
		 
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return null;
		}
		
		Record32KBytes rec_inserted = null;
		try
		{ 
			rec_inserted = this.table_map.get(table).insertRecord(record);  
		}
		finally
		{
			/*
			 * the final segment may be interrupted by Ctrl+C, 
			 * when user closes process on cmd console.
			 * So here use the atomic critical_reference to 
			 * tell shutDown()(or other functions which may interrupt this) 
			 * waiting for a moment.
			 */
			this.current_status = DBRuntimeStatus.onWaiting;
			critical_in_writing.getAndDecrement();
			
		}
		 
		return rec_inserted;
	}

	
	public Record32KBytes[] insertRecord(String table, String[] records) {
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return null;
		
		if(shutdown_called.get())
			return null;
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_in_writing.get()>0)
			;
		
		
		critical_in_writing.getAndIncrement();
		this.current_status = DBRuntimeStatus.onWriting;
		
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR]: Does not contain table " + table);
			return null;
		}
		
		Record32KBytes[] all_inserted = null;
		try
		{
			all_inserted = this.table_map.get(table).insertRecord(records, this.writer_thread_executor);
		} 
		finally
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			critical_in_writing.getAndDecrement();  
		}
		return all_inserted;
		
	}

	public boolean updateRecord(String table, int rec_id, String[] property, String[] new_val) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				/*
				 * only support deleting one by one, low efficient, 
				 * but for data safety and MVCC concerns:
				 * 
				 * If some other thread tries to update one record, 
				 * but it is deleting or updating, than that thread will not get 
				 * control of this record. When deleted, the update will fail, 
				 * and the thread will be noticed.  
				 * 
				 * to be continue...
				 */
				|| this.current_status == DBRuntimeStatus.onDeleting
				|| this.current_status == DBRuntimeStatus.onUpdating)
			return false;
		
		if(shutdown_called.get())
			return false;
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_in_writing.get()>0)
			;

		critical_in_writing.getAndIncrement(); 
		this.current_status = DBRuntimeStatus.onUpdating;
		 
		boolean succeed = false;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return false;
		}
		
		succeed = this.table_map.get(table).updateRecord(rec_id, property, new_val);
	  
		 
		
		this.current_status = DBRuntimeStatus.onWaiting;
		critical_in_writing.getAndDecrement(); 
		
		return succeed;
	}

	/*
	 * A query is a combination of column and its value: 
	 * column:payment,
	 * value:500 
	 * 
	 * LunarDB returns a list of records which are the payment
	 * records that have values 500: 
	 * {name=jackson, payment=500, age=30}
	 * {name=Lucy, payment=500, age=56} 
	 * {name=John, payment=500, age=25}
	 * {name=Rafael, payment=500, age=36} 
	 * ...
	 */
	public ArrayList<Record32KBytes> query(String table, String column, String column_value, int latest_count) throws IOException, InterruptedException, ExecutionException {
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return null;
		
		if(shutdown_called.get())
			return null; 
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR]: Does not contain table " + table);
			return null;
		}
		
		return this.table_map.get(table).query( column, column_value, latest_count);
		  
		 
	}
	
	/*
	 * set top_count 0 if you want all the ids for property=value
	 */
	@Deprecated
	public int[] queryIDs(String table, String column, String column_value, int latest_count)   {
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return null;
		
		if(shutdown_called.get())
			return null;
		
		 
		 
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR]: Does not contain table " + table);
			return null;
		}
		return this.table_map.get(table).queryIDs( column, column_value, latest_count);
		
		 
	}
 
	  
	 
	 
	 
	 
	public int recordsCount(String table)
	{
		if(shutdown_called.get())
			return -1;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return -1;
		}
		return this.table_map.get(table).recordsCount();
	}
	
	public String dbName()
	{
		return root_path + db_name;
	}
	
	public DBRuntimeStatus getStatus()
	{
		return this.current_status;
	}
	
	 
	public ArrayList<Record32KBytes> fetchRecords(String table, int[] record_array) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return  null;
		
		if(shutdown_called.get())
			return null;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return null;
		}
		return this.table_map.get(table).fetchRecords(record_array);
	}
	
	public ArrayList<Record32KBytes> fetchRecords(String table, 
										int[] record_array, 
										int from,
										int count) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return  null;
		
		if(shutdown_called.get())
			return null;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR]: Does not contain table " + table);
			return null;
		}
		 
		return this.table_map.get(table).fetchRecords(record_array, from, count);
	}
	
	public ArrayList<Record32KBytes> fetchRecords(String table, int from, int count) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return  null;
		
		if(shutdown_called.get())
			return null;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return null;
		}
		return this.table_map.get(table).fetchRecords(from, count);
	}
	
	
	public ArrayList<Record32KBytes> fetchLatestRecords(String table, int latest_n) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return  null;
		
		if(shutdown_called.get())
			return null;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return null;
		}
		return this.table_map.get(table).fetchLatestRecords(latest_n);
	}
	
	
	public void save() {
		if(this.current_status == DBRuntimeStatus.closed
				/*|| this.current_status == Status.onClosing*/)
			return  ;
		
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_in_writing.get()>0)
			;
			//System.out.println("waiting critical task finish @LunarDB.save()");
 

		critical_in_writing.getAndIncrement();  
		this.current_status = DBRuntimeStatus.onWriting; 
		
		 
	  
		Iterator<String> it0 = table_map.keySet().iterator();
		while(it0.hasNext())
		{
			String key = it0.next(); 
			LunarTable t_table = table_map.get(key);
			try {
				t_table.save(this.writer_thread_executor);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(view_map != null)
		{
			Iterator<String> it1 = view_map.keySet().iterator();
			while(it1.hasNext())
			{
				String key = it1.next(); 
				LunarMView t_view = view_map.get(key);
				try {
					t_view.save(this.writer_thread_executor); 
				} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
			}
		}
		 
		this.current_status = DBRuntimeStatus.onWaiting;
		critical_in_writing.getAndDecrement();
	}

	public void closeDB() throws IOException {
		 
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return;
		
		/*
		 * then no other thread can let this object do a new operation, 
		 * in other words, critical_in_writing will not increase.
		 */
		if(!shutdown_called.get())
			shutdown_called.set(true);
		
		/*
		 * waiting the running tasks finish.
		 */
		while(critical_in_writing.get()>0)
			;
			//System.out.println("waiting critical task finish @LunarDB.closeDB()");

		
		this.current_status = DBRuntimeStatus.onClosing;
		
		System.out.println("[INFO] LunarBase Engine is shutting down now........");
		
		db_meta.updateMeta(db_meta.getVersion(), -1);
		db_meta.close();
		
		
	 
		
		 
		if(table_map != null)
		{
			Iterator<String> it0 = table_map.keySet().iterator();
			while(it0.hasNext())
			{
				String key = it0.next(); 
				LunarTable t_table = table_map.get(key);
				try {
				t_table.save(this.writer_thread_executor);
				t_table.closeTable();
				} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
			}
		}
		if(view_map != null)
		{
			Iterator<String> it0 = view_map.keySet().iterator();
			while(it0.hasNext())
			{
				String key = it0.next(); 
				LunarMView t_view = view_map.get(key);
				try {
					t_view.save(this.writer_thread_executor);
					t_view.closeView();
				} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
			}
		}
		
		
		
		this.current_status = DBRuntimeStatus.closed;
		System.out.println("[SUCCEED]: LunarBase exited OK........");
	}

}

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
import LunarX.Node.Grammar.AST.Relational.ExpressionNode;
import LunarX.Node.Grammar.AST.Relational.Visitor.ExecutorVisitor;
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

public class LunarXNode {
	
	final long lunarbase_version = 1L;
	DBFSProperties dbfs_prop_instance;
	XNodeMeta db_meta;
	TablePath table_path_builder;
	
	String root_path;
	String db_name;
	  
	
	HashMap<String, LunarTable> table_map;  
	HashMap<String, LunarMView> view_map;  
	/*
	 * <node id, <table name, LunarTable>> 
	 */
	//HashMap<Integer, HashMap<String, LunarTable>> virtual_node_map;
	/*
	 * <table name, <integer node ids>>
	 */
	//HashMap<String, Vector<Integer>> table_vnodes_map;
	
	QuickSort sorter;
	
	
	protected ExecutorService writer_thread_executor;
	protected ExecutorService tokenizer_thread_executor;
	protected ExecutorService reader_thread_executor;
	 
	XNodeStatus.DBRuntimeStatus current_status;
	protected XNodeShutdownHook hook;
	protected AtomicBoolean shutdown_called;
	protected AtomicLong  critical_in_writing;

 
	private static class LunarDBInstance {
		private static final LunarXNode g_db_instance = new LunarXNode();
	}


	//this is a singleton pattern, if you want a global only one instance.
	
	public static LunarXNode getInstance() {
		return LunarDBInstance.g_db_instance;
	}
	 
	/*
	 * be noticed, this is a public constructor, 
	 * the above singleton maybe lose effect if we call this 
	 * constructor directly.
	 */
	public LunarXNode() {
		this.current_status = DBRuntimeStatus.closed;
	}

	public boolean createDB(String _root_path_to_create, String creation_conf) {
		
		this.shutdown_called = new AtomicBoolean(false);
		this.critical_in_writing = new AtomicLong(0);
		
		
		current_status = XNodeStatus.DBRuntimeStatus.onCreating;
		dbfs_prop_instance = new DBFSProperties(_root_path_to_create);
		dbfs_prop_instance.loadCriticalValues( creation_conf);
		dbfs_prop_instance.loadConfig( creation_conf);

		table_path_builder = new TablePath(dbfs_prop_instance);
		root_path = dbfs_prop_instance.db_root_path;
		db_name = dbfs_prop_instance.db_name;

		if (this.root_path == null || db_name == null) {
			System.err.println("[ERROR]: database name is invalid, quit creating.");
			return false;
		}
		if (!root_path.endsWith("/"))
			root_path = root_path + "/";

		File db_root_dir = new File(root_path + db_name);

		if (!db_root_dir.exists() && !db_root_dir.isDirectory()) {
			db_root_dir.mkdir();
			dbfs_prop_instance.createConfig(creation_conf);
			 
				String runtime_conf_file = db_root_dir + "/" + DBFSProperties.runtime_conf;

				dbfs_prop_instance.loadCriticalValues(runtime_conf_file);
				dbfs_prop_instance.loadConfig(runtime_conf_file);
  

			File db_rec_table_dir = new File(table_path_builder.getTablePath());
			db_rec_table_dir.mkdir();
			File mat_view_dir = new File(table_path_builder.getViewPath());
			mat_view_dir.mkdir();

			try {
				db_meta = new XNodeMeta( root_path + "/" + db_name ,
								dbfs_prop_instance.bit_buff_len );
				
				db_meta.updateMeta(lunarbase_version, -1);
				db_meta.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				db_root_dir.delete();
				current_status = XNodeStatus.DBRuntimeStatus.closed; 
				
			}
			
			
			System.out.println("[SUCCEED]: LunarBase: " + root_path + db_name + ", is successfully created");
		} else {
			System.err.println("[ERROR]: database already exists, fail creating!");
			System.err.println("[TRY]: rename your database, and try again.");
			current_status = XNodeStatus.DBRuntimeStatus.closed; 
			return false;
		}

		current_status = XNodeStatus.DBRuntimeStatus.closed; 
		return true;

	}

	public boolean hasTable(String table_name)
	{ 
		if(this.table_map.get(table_name)==null)
			return false;
		
		return true; 
	}
	
	public boolean hasMView(String view_name)
	{ 
		if(this.view_map.get(view_name)==null)
			return false;
		
		return true; 
	}
	
	
	protected boolean makeDir(File dir)
	{
		if( dir.exists() && dir.isDirectory())
		{
			System.err.println("[ERROR]: " + dir + " already exists, fail creating!");
			System.err.println("[TRY]: rename your table, and try again.");
			return false;
		}
		dir.mkdir();
		
		System.out.println("[INFO]: table directory is: " + dir.getAbsolutePath());
		
		File conf = new File(dir.getAbsolutePath() + EnginPathDefinition.record_table_rt_conf);
		try {
			conf.createNewFile();
		
		} catch (IOException e) { 
			e.printStackTrace();
			return false;
		} 
		
		return true; 
	}
	
	/*
	 * create a local table, without node id, so it can not 
	 * be distributed over several machines. 
	 */
	public boolean createTable(String table_name)
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
	public boolean createMaterializedView(String view_name)
	{ 
		File _view_dir = new File(table_path_builder.getViewPath() + view_name);
		return makeDir(_view_dir);
	}
	
	public boolean openTable(String table_name)  
	{ 

		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return false;
		
		if(shutdown_called.get())
			return false;
		
		 

		critical_in_writing.getAndIncrement();  
		this.current_status = DBRuntimeStatus.onWriting; 
		
		try
		{
			File _table_dir = new File(table_path_builder.getTablePath() + table_name);
			
			if(_table_dir.isDirectory())
			{ 
				LunarTable one_table = new LunarTable();
				boolean succeed = false;
				succeed = one_table.openTable(_table_dir.getAbsolutePath(), 
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
	
	public boolean openDB(String _root_path_with_db_name)
	{
		String t_path;
		if (!_root_path_with_db_name.endsWith("/"))
			t_path = StrLegitimate.purifyStringEn(_root_path_with_db_name) + "/";
		else
			t_path = StrLegitimate.purifyStringEn(_root_path_with_db_name);
		
		String[] arr = t_path.split("/");
		String _root_path = "";
		for(int i=0;i<arr.length-1;i++)
		{
			if(!"".equals(arr[i]))
				_root_path =  _root_path + "/" + arr[i]  ;
		}
		
		String _db_name = arr[arr.length-1];
		return openDB(_root_path, _db_name);
	}
	
	
	public boolean openDB(String _root_path, String _db_name)  {
		if(this.current_status != DBRuntimeStatus.closed )
		{
			System.err.println("[ERROR]: Another database is opened by lunarbase engine!");
			System.err.println("[INFO]: Please close it first and try open " + _root_path + _db_name + " again.");
			return false;
		}
		
		this.current_status = DBRuntimeStatus.onOpening;
		this.shutdown_called = new AtomicBoolean(false);
		this.critical_in_writing = new AtomicLong(0);
		
		this.hook = new XNodeShutdownHook(this);
		Runtime.getRuntime().addShutdownHook(this.hook);  

		String t_path;
		if (!_root_path.endsWith("/"))
			t_path = StrLegitimate.purifyStringEn(_root_path) + "/";
		else
			t_path = StrLegitimate.purifyStringEn(_root_path);

		 
		String conf_file_name = t_path + _db_name + "/" + DBFSProperties.runtime_conf;
		File conf_file = new File(conf_file_name);
		if(!conf_file.exists())
		{
			System.err.println("[ERROR]: database " + t_path + _db_name  + " does not exist, fail opening");
			return false;
		}
		dbfs_prop_instance = new DBFSProperties(t_path);	
		dbfs_prop_instance.loadCriticalValues(conf_file_name);
		dbfs_prop_instance.loadConfig(conf_file_name);

		table_path_builder = new TablePath(dbfs_prop_instance);
		root_path = dbfs_prop_instance.db_root_path;
		db_name = dbfs_prop_instance.db_name;
		
		/*
		 * check the version and owner id
		 */
		try {
			db_meta = new XNodeMeta(t_path + _db_name , dbfs_prop_instance.bit_buff_len);
		
			if(this.lunarbase_version < db_meta.getVersion())
			{
				System.err.println("[ERROR]: Database: " 
								+ t_path + _db_name  
								+ " has been created by a newer version of LunarBase.");
				System.err.println("[Try]: Please update your LunarBase.");
				db_meta.close();
				return false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			db_meta.close();
			return false;
		}
		
		
		if( db_meta.getOwner() > -1)
		{
			System.err.println("[ERROR]: Database " + t_path + _db_name  + " possiblelly has another owner: ");
			System.err.println("        1. it is in use by another process now, or ");
			System.err.println("        2. it has been closed abnormally because of system failure.");
			System.err.println("[Try]: Please :");
			System.err.println("        1. make sure there is no other process opening this database.");
			System.err.println("        2. if it is, close that process and open this database again.");
			System.err.println("        3. if it is not, delete the ldb.lock file under " 
											+ _root_path +", and open the database again.");
			db_meta.close();
			return false;
		}
		try {
			db_meta.updateMeta(db_meta.getVersion(), Thread.currentThread().getId());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		 
		
		table_map = new HashMap<String, LunarTable>();
		 
		File _table_dir = new File(table_path_builder.getTablePath());
		if (_table_dir.exists() && _table_dir.isDirectory()) 
		{
			File dir[] = _table_dir.listFiles();
			if(dir.length>=1)
			{
				for(int j=0;j<dir.length;j++)
				{
					if(dir[j].isDirectory())
						this.openTable(dir[j].getName());
				}
			}
		}
		
		view_map = new HashMap<String, LunarMView>();
		
		File _view_dir = new File(table_path_builder.getViewPath());
		if ( _view_dir.exists() && _view_dir.isDirectory()) 
		{
			File view_dir[] = _view_dir.listFiles();
			if(view_dir.length>=1)
			{
				for(int j=0;j<view_dir.length;j++)
				{
					if(view_dir[j].isDirectory())
						try {
							this.openMaterializedView(view_dir[j].getName());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
			} 
		}
		
	 
		//this.writer_thread_executor = Executors.newSingleThreadExecutor();
		final int parallel = Runtime.getRuntime().availableProcessors() ; 
		this.writer_thread_executor = Executors.newFixedThreadPool(parallel) ;
		this.tokenizer_thread_executor = null; 
		this.reader_thread_executor = Executors.newFixedThreadPool(dbfs_prop_instance.concurrent_level-1);
		sorter = new QuickSort();
		 
		this.current_status = DBRuntimeStatus.onWaiting;
		
		System.out.println("[SUCEED]: Database " + t_path + _db_name  + " is running now.......");
		
		return true;

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
			all_inserted = this.table_map.get(table).insertRecord(records, writer_thread_executor);
		} 
		finally
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			critical_in_writing.getAndDecrement();  
		}
		return all_inserted;
		
	}

	public boolean appendFulltextIndexFor(String table, 
			int rec_id, String column, String extra_content)  
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return false;
		
		if(shutdown_called.get())
			return false; 
		
		
		critical_in_writing.getAndIncrement();
		this.current_status = DBRuntimeStatus.onWriting;
		
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR]: Does not contain table " + table);
			return false;
		}
		
		 
		try
		{
			if( tokenizer_thread_executor == null)
				 tokenizer_thread_executor = Executors.newFixedThreadPool(dbfs_prop_instance.concurrent_level-1);
			
			this.table_map.get(table).appendFulltextIndexFor(rec_id, column, extra_content, tokenizer_thread_executor); 
		} 
		finally
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			critical_in_writing.getAndDecrement();  
		}
		return true;
		
	}
	
	public boolean deleteRecord(String table, int rec_id) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				/*
				 * only support deleting one by one, low efficient, 
				 * but for data safety and MVCC concerns:
				 * 
				 * If some other thread tries to update one record, 
				 * but it is deleting, than that thread will not get 
				 * control of this record. When deleted, the update will fail, 
				 * and the thread will be noticed. 
				 *  
				 * to be continue...
				 */
				|| this.current_status == DBRuntimeStatus.onDeleting)
			return false;
		
		if(shutdown_called.get())
			return false; 

		critical_in_writing.getAndIncrement();  
		this.current_status = DBRuntimeStatus.onDeleting;
		
		boolean succeed = false;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR]: Does not contain table " + table);
			critical_in_writing.getAndDecrement(); 
			return false;
		}
		
		succeed = this.table_map.get(table).deleteRecord(rec_id);
	 
		this.current_status = DBRuntimeStatus.onWaiting;
		critical_in_writing.getAndDecrement(); 
		
		return succeed;
	}

	public boolean updateRecord(String table, int rec_id, String[] columns, String[] new_val) 
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
		
	 

		critical_in_writing.getAndIncrement(); 
		this.current_status = DBRuntimeStatus.onUpdating;
		 
		boolean succeed = false;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return false;
		}
		
		succeed = this.table_map.get(table).updateRecord(rec_id, columns, new_val);
	  
		 
		
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
	 * String exp = "O[R[score,75,98,1,1], R[score, -10000, 50,0,0]]";
	 * stands for such a relationship:
	 * 
	 * S.\"score\" between 75 and 98 or S.\"score\" < 50
	 *
	 * @ExpressionNode for the comments of algebraic statement.
	 */
	public FTQueryResult queryRelational(String table, String algebraic_statement)  
	{
		ExpressionNode e_node = new ExpressionNode(algebraic_statement,0);
		int end_index = e_node.extract();
		if(end_index < 0)
			return new FTQueryResult(this, table, null);
		
		ExecutorVisitor ev = new ExecutorVisitor(this,table);
		
		return ev.visit(e_node);

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
	
	/*
	 * set top_count 0 if you want all the ids for column containing keyword 
	 */
	public FTQueryResult queryFullText(String table, 
									String column, 
									String keyword, 
									int latest_count)   
	{
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
		
		int[] result = this.table_map.get(table).queryFullText( column, keyword, latest_count, this.reader_thread_executor);
		return new FTQueryResult(this, table, result);
		 
	}
	
	public FTQueryResult  queryFullText(String table, 
										String statement,
										int latest_count)  
	{
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
		//return this.table_map.get(table).queryFullText(statement, latest_count);
		//Set<Integer> id_set = this.table_map.get(table).queryFullText(statement, latest_count);
		HashMap<Integer, Integer> id_set = this.table_map.get(table).queryFullText(statement, latest_count, this.reader_thread_executor);
		
		int top_n = latest_count;
		return new FTQueryResult(this, table,  id_set, top_n); 
	}
	
	/*
	 * by default, the keys of start and end are inclusive:  key_start <= col <= key_end
	 */
	public FTQueryResult queryRange (String table, 
			String column, 
			int key_start, 
			int key_end) 
	{
		return this.queryRange (table, column, (long)key_start, (long)key_end); 
	}
	
	public FTQueryResult queryRange (String table, 
			String column, 
			int key_start, 
			int key_end,
			boolean lower_inclusive,
			boolean upper_inclusive) 
	{ 
		return this.queryRange (table, column, (long)key_start, (long)key_end, lower_inclusive, upper_inclusive); 
	}
	 
	/*
	 * by default, the keys of start and end are inclusive:  key_start <= col <= key_end
	 */
	public FTQueryResult queryRange(String table, 
									String column, 
									long key_start, 
									long key_end) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return null;
		
		if(shutdown_called.get())
			return null;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return null;
		}
		 
		int[] result = this.table_map.get(table).queryRangeIDs( column, key_start, key_end);
		return new FTQueryResult(this, table, result);
		 
	}
	
	public FTQueryResult queryRange(String table, 
			String column, 
			long key_start, 
			long key_end,
			boolean lower_inclusive,
			boolean upper_inclusive) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return null;
		
		if(shutdown_called.get())
			return null;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return null;
		}
		
		long lower = key_start;
		long upper = key_end;
		if(!lower_inclusive)
			lower += 1;
		if(!upper_inclusive)
			upper -= 1;
		
		int[] result = this.table_map.get(table).queryRangeIDs( column, lower, upper);
		return new FTQueryResult(this, table, result);
	
	}
	/*
	public int[][] queryRangeIDs(String table, String property, float key_start, float key_end) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return null;
		
		if(shutdown_called.get())
			return null;
		
		if(this.table_map.get(table) == null)
		{
			this.current_status = DBRuntimeStatus.onWaiting;
			System.err.println("[ERROR] Does not contain table " + table);
			return null;
		}
		return this.table_map.get(table).queryRangeIDs( property, key_start, key_end) ;
		 
	}
*/
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
	
	public ArrayList<Record32KBytes> fetchRecordsASC(String table, int from, int count) throws IOException
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
		return this.table_map.get(table).fetchRecordsASC(from, count);
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
		
		
		 
		if (this.writer_thread_executor != null)
			this.writer_thread_executor.shutdown();
 	 
		if (this.tokenizer_thread_executor != null) 
			this.tokenizer_thread_executor.shutdown(); 
		
		if (this.reader_thread_executor != null)
			this.reader_thread_executor.shutdown();
	 	 
		
		 
		if(table_map != null)
		{
			Iterator<String> it0 = table_map.keySet().iterator();
			while(it0.hasNext())
			{
				String key = it0.next(); 
				LunarTable t_table = table_map.get(key);
				try {
					//t_table.save(this.writer_thread_executor);
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
					//t_view.save(this.writer_thread_executor);
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

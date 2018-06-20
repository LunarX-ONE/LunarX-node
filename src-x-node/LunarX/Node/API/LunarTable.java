/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG
 * 
 * @author LunarBase team, contact: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import LCG.DB.Table.LunarLHTEngine;
import LCG.EnginEvent.Interfaces.LFuture;
import LCG.FSystem.Def.DBFSProperties;
import LCG.MemoryIndex.IndexTypes.DataTypes;
import LCG.MemoryIndex.LunarMaxWithStorage;
import LCG.MemoryIndex.IndexTypes;
import LCG.Utility.StrLegitimate;
import LunarX.Memory.Closable;
import LunarX.Memory.TableMemoryStore;
import LunarX.Node.API.XNodeStatus.DBRuntimeStatus;
import LunarX.Node.Conf.EnginPathDefinition;
import LunarX.Node.Conf.TableConf;
import LunarX.Node.Conf.TableConf.ColumnClassified;
import LunarX.Node.Grammar.AST.Expression.Expression;
import LunarX.Node.Grammar.AST.Expression.ExpressionKeywords;
import LunarX.Node.Grammar.AST.Visitor.BinaryLogicalVisitor;
import LunarX.Node.Grammar.Parser.LexerSubExpression;
import LunarX.Node.ThreadTasks.DBTaskColumnIndexer;
import LunarX.Node.ThreadTasks.DBTaskSaveColumnIndex;
import LunarX.Node.ThreadTasks.SETaskColumnIndexAppender;
import LunarX.Node.ThreadTasks.SETaskColumnIndexer;
import LunarX.Node.ThreadTasks.SETaskColumnIndexerVOld;
import LunarX.Node.ThreadTasks.SETaskSearchIDs;
import LunarX.Realtime.Column.LunarMaxPersistent;
import LunarX.RecordTable.RecordHandlerCenter;
import LunarX.RecordTable.TableReader;
import LunarX.RecordTable.RecStatusUtile.RecStatus;
import LunarX.RecordTable.StoreUtile.LunarColumn;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
import Lunarion.SE.AtomicStructure.TermScore;
import Lunarion.SE.FullText.Lexer.NoTokenizer;
import Lunarion.SE.FullText.Lexer.TokenizerInterface;

public class LunarTable extends Closable{
	
	String table_root_path;
	private String table_name;
	String table_conf; 
	TableConf t_conf;
	DBFSProperties dbfs_prop_instance;
	
	//LunarLHTEngin lunar_lht_engin = null;
	/* 
	 * must be noticed that this hashmap has columns coming from 
	 * rt_searchable=varchar\:table.column 
	 * and 
	 * fulltext_searchable=table.another_comment
	 * 
	 * */
	HashMap<String, LunarLHTEngine> column_fulltext_indexer_map;  
	
	RecordHandlerCenter record_writer;
	
	TableReader record_reader;
	TableMemoryStore memory_table; 
	 
	//LunarMax lunar_max = null;
	//LunarMaxPersistent lm_persistent = null;
	LunarMaxWithStorage lm_storage = null;
	
	/*
	protected ExecutorService writer_thread_executor;
	protected ExecutorService tokenizer_thread_executor;
	protected ExecutorService reader_thread_executor;
	*/
	Future<List<Record32KBytes>> fulltext_index_future;
	
	//Future fulltext_index_future;
	
	/*
	 * the blocking queue used here is for keeping the records that 
	 * wait for indexing. 
	 * Since when records coming, they will be flushed sequentially to the disk, 
	 * and after that, it will be indexed one column by another. 
	 * 
	 * Building index, including full-text indexing, B+ indexing or else, 
	 * is time consuming, both CPU and IO intensive.
	 * Therefore, indexing task is sent to other thread to finish.
	 * 
	 * Then the functions of insertion records called here can return and accept next call.  
	 * 
	 * This non-blocking way is nice when records rush to database, 
	 * we still have good response, and have a room to put these records first. 
	 * The back-end indexing thread just accepts records and index them, 
	 * waiting the records flood passing.
	 * Since in real application, there will not be constantly high traffic throughput.
	 * The back-end thread has enough time to finish its job. 
	 * 
	 */
	protected final int records_queue_size = 1024; /* shall be bigger? */
	BlockingQueue<Record32KBytes> records_queue;
	//BlockingQueue<Record32KBytes[]> records_for_fulltext_index;
	BlockingQueue<List<Record32KBytes>> records_failed_in_fulltext_index;
	
	BinaryLogicalVisitor b_l_visitor;
	//List<String> records_failed;
	
	XNodeStatus.DBRuntimeStatus current_status;
	boolean real_time_mode = false; 

	public LunarTable() {
		this.current_status = DBRuntimeStatus.closed;
	} 
	
	public boolean removeTable( )
	{
		if(this.current_status != DBRuntimeStatus.closed )
		{
			System.err.println("[ERROR]: Table " + this.table_name + " is in use, and can not be removed! @LunarTable.removeTable()");
			System.err.println("[INFO]: Please close it first and try to remove " + this.table_root_path + " again.");
			return false;
		}
		
		critical_reference.getAndIncrement(); 
		 
		
		File table_file = new File(table_root_path);
		table_name = table_file.getName();
		if(!table_file.exists())
		{
			System.err.println("[ERROR]: Table " + table_root_path + " does not exist, fail to remove @LunarTable.removeTable().");
			critical_reference.getAndDecrement();
			
			return false;
		} 
		
		
		table_conf = table_root_path.substring(0, table_root_path.length()-1) + EnginPathDefinition.record_table_rt_conf;
		if(t_conf != null)
		{	
			try {
				t_conf.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.err.println("[ERROR]: Table " + table_root_path + " configure file is in use, fail to remove @LunarTable.removeTable().");
				critical_reference.getAndDecrement();
				
				return false;
			}
		}
		
		t_conf = new TableConf(table_conf);
		t_conf.loadConfig();
		
		if(t_conf.isSetRemoved())
		{
			critical_reference.getAndDecrement();
			return false; 
		}
		else
		{	
			try {
				t_conf.setTableRemoved(table_name);
			} catch (IOException e) {
				e.printStackTrace();
				critical_reference.getAndDecrement();
				return false;
			}
		}
		critical_reference.getAndDecrement();
		System.err.println("[INFO]: table "+ table_name +" has been removed successfully.");
		
		return true;
		
		
	}
	
	public boolean restoreTable( )
	{
		if(this.current_status != DBRuntimeStatus.removed )
		{
			System.err.println("[ERROR]: Table " + this.table_name + " is in use @LunarTable.restoreTable(), and need not to be restored!");
			return false;
		}
		
		critical_reference.getAndIncrement(); 
		
		File table_file = new File(table_root_path);
		table_name = table_file.getName();
		if(!table_file.exists())
		{
			System.err.println("[ERROR]: Table " + table_root_path + " does not exist, fail to restore it @LunarTable.restoreTable().");
			critical_reference.getAndDecrement();
			
			return false;
		} 
		
		
		table_conf = table_root_path.substring(0, table_root_path.length()-1) + EnginPathDefinition.record_table_rt_conf;
		
		if(t_conf != null)
		{	
			try {
				t_conf.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.err.println("[ERROR]: Table " + table_root_path + " configure file is in use, fail to restore @LunarTable.restoreTable().");
				critical_reference.getAndDecrement();
				
				return false;
			}
		}
		t_conf = new TableConf(table_conf);
		t_conf.loadConfig();
		
		if(!t_conf.isSetRemoved())
		{
			critical_reference.getAndDecrement();
			return false; 
		}
		else
		{	
			try {
				t_conf.setTableRestored(table_name);
				this.current_status = DBRuntimeStatus.closed;
			} catch (IOException e) {
				e.printStackTrace();
				critical_reference.getAndDecrement();
				return false;
			}
		}
		critical_reference.getAndDecrement();
		System.err.println("[INFO]: table "+ table_name +" has been restored successfully.");
		
		return true;
		
		
	}
	
	
	public boolean openTable(String _table_path,
								boolean _real_time_mode, 
								int _max_recs_in_memory,
								DBFSProperties _dbfs_prop_instance
								)  
	{
		if(this.current_status != DBRuntimeStatus.closed )
		{
			System.err.println("[ERROR]: Table " + this.table_name + " has already opened by lunarbase engine! @LunarTable.openTable()");
			System.err.println("[INFO]: Please close it first and try to open " + _table_path + " again.");
			return false;
		}
		dbfs_prop_instance = _dbfs_prop_instance;
		shutdown_called = new AtomicBoolean(false);
		critical_reference = new AtomicLong(0);
		 
		critical_reference.getAndIncrement();
		
		if (!_table_path.endsWith("/"))
			table_root_path = StrLegitimate.purifyStringEn(_table_path) + "/";
		else
			table_root_path = StrLegitimate.purifyStringEn(_table_path); 
		 
		File table_file = new File(table_root_path);
		table_name = table_file.getName();
		if(!table_file.exists())
		{
			System.err.println("[ERROR]: Table " + table_root_path + " does not exist, fail opening");
			critical_reference.getAndDecrement();
			this.current_status = DBRuntimeStatus.closed;
			return false;
		} 
		
	 
		
		this.real_time_mode = _real_time_mode;
		
		memory_table = new TableMemoryStore(_max_recs_in_memory, dbfs_prop_instance.concurrent_level);
		
		try {
			record_writer = new RecordHandlerCenter(table_file, memory_table, _dbfs_prop_instance); 
			record_reader = new TableReader(memory_table, record_writer); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			critical_reference.getAndDecrement();
			this.current_status = DBRuntimeStatus.closed;
			return false;
			
		}
		table_conf = table_root_path.substring(0, table_root_path.length()-1)  + EnginPathDefinition.record_table_rt_conf;
		t_conf = new TableConf(table_conf);
		t_conf.loadConfig();
		
		if(t_conf.isSetRemoved())
		{
			this.current_status = DBRuntimeStatus.removed;
			critical_reference.getAndDecrement(); 
			return false;
		}
		//if(DBFSProperties.full_index_mode)
		//	lunar_lht_engin = new LunarLHTEngin(root_path, db_name);
		
		column_fulltext_indexer_map = new HashMap<String, LunarLHTEngine>();
		
		if(this.real_time_mode)
		{	
			//lm_persistent = new LunarMaxPersistent(dbfs_prop_instance.rt_virtual_mem_enabled,
			//										record_writer);
			lm_storage = new LunarMaxWithStorage(record_writer.getFSProperties().rt_virtual_mem_enabled, 
					record_writer.getFSProperties().rt_precision, 
					record_writer.getTablePath(), 
					record_writer.getFSProperties().rt_max_memory, 
					record_writer.getFSProperties().rt_max_virtual_pte, 
					24,
					record_writer.getFSProperties().rt_threads) ;
			
			if(t_conf.rt_searchable_tables != null)	
			{			
				for(int i=0;i< t_conf.rt_searchable_tables.length;i++)
				{  
					if(this.table_name.equals(t_conf.rt_searchable_tables[i]))
					{
						/*
						lm_persistent.registerPersisitent
							(this.table_name, t_conf.rt_searchable_columns[i], t_conf.rt_searchable_data_types[i] );
			
						lm_persistent.loadRecords( t_conf.rt_searchable_columns[i]);
						*/
						
						if(DataTypes.VARCHAR == t_conf.rt_searchable_data_types[i])
						{
							LunarLHTEngine lunar_lht_engin_for_varchar;
							try {
								lunar_lht_engin_for_varchar = new LunarLHTEngine(table_root_path,
																					t_conf.rt_searchable_columns[i],
																					DataTypes.VARCHAR, 
																					dbfs_prop_instance);
								column_fulltext_indexer_map.put(t_conf.rt_searchable_columns[i], 
																lunar_lht_engin_for_varchar);
								
								 
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}  
						}
						else
						{
							lm_storage.registerIndexer
							(this.table_name, t_conf.rt_searchable_columns[i], t_conf.rt_searchable_data_types[i] );
							
						} 
					} 
				}  
			} 
		}
		
		
		if(t_conf.fulltext_searchable_columns != null)
		{ 
			for(int i=0;i< t_conf.fulltext_searchable_columns.length;i++)
			{
				LunarLHTEngine lunar_lht_engin;
				try {
					lunar_lht_engin = new LunarLHTEngine(table_root_path,
							t_conf.fulltext_searchable_columns[i], 
							DataTypes.STRING,
							dbfs_prop_instance);
					column_fulltext_indexer_map.put(t_conf.fulltext_searchable_columns[i], 
							lunar_lht_engin);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			} 
		}
		
		/*
		this.writer_thread_executor = Executors.newSingleThreadExecutor();
		this.tokenizer_thread_executor = null;
		this.reader_thread_executor = Executors.newFixedThreadPool(dbfs_prop_instance.concurrent_level-1);
		*/
		
		records_queue = new LinkedBlockingQueue<Record32KBytes>(records_queue_size);
		//records_for_fulltext_index  = new LinkedBlockingQueue<Record32KBytes[]>(records_queue_size);
		records_failed_in_fulltext_index = new LinkedBlockingQueue<List<Record32KBytes>>( );
		
		//fulltext_index_future = this.writer_thread_executor
				//.submit(new SETaskColumnIndexer(records_for_fulltext_index, this.column_fulltext_indexer_map));
		//this.writer_thread_executor.submit(new SETaskColumnIndexerVOld(records_for_fulltext_index,  
		//											records_failed_in_fulltext_index,
		//											column_fulltext_indexer_map));
		
		critical_reference.getAndDecrement();
		
		//b_l_visitor = new BinaryLogicalVisitor(this, );
		b_l_visitor = null;
		this.current_status = DBRuntimeStatus.onWaiting;
		return true;

	}
	
	
	public boolean addSearchable(String data_type, String column) throws IOException  
	{
		critical_reference.getAndIncrement();
		boolean succeed = false;
		
		if(DataTypes.STRING ==  IndexTypes.getDatatype(data_type)
				|| DataTypes.TEXT == IndexTypes.getDatatype(data_type))
		{
			succeed = this.addFulltextSearchable(column);
		}
		/*
		 * for varchar, since it is not numeric, use search engine for search, 
		 * but this type has no parser, its value is a key.
		 */
		else if(DataTypes.VARCHAR ==  IndexTypes.getDatatype(data_type))
		{
			succeed = t_conf.addSearchable(  data_type, this.table_name, column); 
			 
			if(this.column_fulltext_indexer_map.get(column)!=null)
			{
				System.out.println("[WARNING]: there is already a full text index on column " 
										+ column);
				critical_reference.getAndDecrement();
				return false;
			}
			
			LunarLHTEngine lunar_lht_engin = null;
			try {
				lunar_lht_engin = new LunarLHTEngine(this.table_root_path, column, DataTypes.VARCHAR, dbfs_prop_instance);
				lunar_lht_engin.flush();
				 
			} catch (IOException e) {
				critical_reference.getAndDecrement();
				e.printStackTrace();
				return false;
			} 
		 
			this.column_fulltext_indexer_map.put(column, lunar_lht_engin);
		}
		else
		{
			succeed = t_conf.addSearchable(  data_type, this.table_name, column); 
			/*
			succeed = lm_persistent.registerPersisitent (this.table_name, 
												column, 
												IndexTypes.getDatatype(data_type ) );
												*/
			succeed = lm_storage.registerIndexer(this.table_name, 
												column, 
												IndexTypes.getDatatype(data_type ));
		}
		critical_reference.getAndDecrement();
		
		return succeed;
	}
	
	public boolean addStorable( String column) throws IOException
	{
		critical_reference.getAndIncrement();
		
		boolean succeed = false; 
		
		succeed = t_conf.addStorable( this.table_name, column); 
		 
		critical_reference.getAndDecrement();
		return succeed;
	}
	
	public Iterator<String> getFulltextColumns()
	{
		return this.column_fulltext_indexer_map.keySet().iterator();
	}
	
	public boolean addFulltextSearchable(String column)  
	{
		critical_reference.getAndIncrement();
		boolean succeed = false;
		try {
			succeed = t_conf.addFulltextSearchable(this.table_name, column);
			/*
			 * then in tableColumns() will include the full text searchable column
			 */
			succeed = t_conf.addStorable(this.table_name, column);
		} catch (IOException e) {
			critical_reference.getAndDecrement();
			e.printStackTrace();
			return false;
		}
		
		if(this.column_fulltext_indexer_map.get(column)!=null)
		{
			System.out.println("[WARNING]: there is already a full text index on column " 
									+ column);
			critical_reference.getAndDecrement();
			return false;
		}
		
		LunarLHTEngine lunar_lht_engin = null;
		try {
			lunar_lht_engin = new LunarLHTEngine(this.table_root_path, column, DataTypes.STRING, dbfs_prop_instance);
			lunar_lht_engin.flush();
		} catch (IOException e) {
			critical_reference.getAndDecrement();
			e.printStackTrace();
			return false;
		}
		
		this.column_fulltext_indexer_map.put(column, lunar_lht_engin);
		critical_reference.getAndDecrement();
		return succeed;
	}
 
	/*
	 * never allow multiple columns have one tokenizer, 
	 * since in multi-threaded environment different columns may mix their tokenized results.  
	 */
	public void registerTokenizer(String column, TokenizerInterface tokenizer)
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return;
		
		//Iterator<String> it0 = column_fulltext_indexer_map.keySet().iterator();
		//while(it0.hasNext())
		//{
		//	String column = it0.next(); 
		if(column_fulltext_indexer_map.get(column) != null)
			column_fulltext_indexer_map.get(column).registerTokenizer(tokenizer); 
			 
		//}
	}
	
	
	/*
	 * this also create new Record32KBytes object
	 */
	 
	 
	/*
	 * JSON records like: {name=jackson, payment=500, age=30}
	 */
	public Record32KBytes insertRecord(String record) throws InterruptedException, ExecutionException 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return null;
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_reference.get()>0)
			;
		
		critical_reference.getAndIncrement();
		this.current_status = DBRuntimeStatus.onWriting;
		
		Record32KBytes rec = null;
		boolean succeed = false;
		//this.records_queue.put(record);
		
		/*
		try {
			rec = insertDiskFirst( record);
		} catch (IOException e) { 
			e.printStackTrace();
		}
		*/
		/*
		if(DBFSProperties.full_index_mode)
		{	
			Future<List<String>> future = this.writer_thread_executor
				.submit(new TaskRecInsert(records_queue, lunar_lht_engin));
			
			List<String> failed_record = future.get();
			this.current_status = Status.onWaiting;
			if (failed_record.isEmpty())
				succeed = true;
			
			succeed =  false;

		} 
		*/
		/*
		if(this.real_time_mode)
		{
			try {
				//insertLunarMax(rec);
			} catch (IOException e) { 
				e.printStackTrace();
			} 
		}
		*/
		this.current_status = DBRuntimeStatus.onWaiting;
		 
		critical_reference.getAndDecrement();
		return rec;
	}

	
	public Record32KBytes[] insertRecord(String[] records, ExecutorService writer_thread_executor )   {
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return null;
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_reference.get()>0)
			;
		
		
		critical_reference.getAndIncrement();
		this.current_status = DBRuntimeStatus.onWriting;
		
		Record32KBytes[] succeed_recs = new Record32KBytes[records.length];
		boolean succeed = false;
		for (int i = 0; i < records.length; i++) {
			 
			try {
				 
				succeed_recs[i] =  record_writer.insertRecord(records[i]);
			} catch (IOException e) {
				/*
				 * any record throws an IOException, indicating 
				 * that the system may has some error occur. 
				 * So just stop writing and return, 
				 * and previous inserted recs in this iteration 
				 * will be lost.
				 */
				this.current_status = DBRuntimeStatus.onWaiting;
				critical_reference.getAndDecrement();
				
				e.printStackTrace();
				
				return null;
			}
		} 
		
		/*
		 * make sure this bunch of records flush to disk. 
		 * If fail to flush, in the cases of power failure, 
		 * system crash or something else, then the record_writer will not update 
		 * its meta-data, where keeps important storage information of 
		 * all the inserted records.
		 * 
		 * In this situation, the records inserted above will all lost.
		 * 
		 * see RecordHandlerCenter.rec_index to know what meta-data is in it.
		 * 
		 * this process will make the insertion slow, but more safety. 
		 * 
		 * If user wants a quicker insertion, use a bigger array to cache the 
		 * records. It's up to user. 
		 */
		 
		try {
			record_writer.flushLatestWriter();
		} catch (IOException e) {
			this.current_status = DBRuntimeStatus.onWaiting;
			critical_reference.getAndDecrement();
			
			e.printStackTrace();
			
			return null;
		}
		 
		 
		
		if(this.real_time_mode)
		{
			Iterator<String> columns = lm_storage.columnIterator(); 
			List<Future<List<Record32KBytes>>> results = null;
			if(columns.hasNext())
			{	results = new ArrayList<Future<List<Record32KBytes>>>();
			
				while(columns.hasNext())
				{
					String col = columns.next();
					Future<List<Record32KBytes>> _future =  writer_thread_executor 
							.submit(new DBTaskColumnIndexer(this, lm_storage,col, succeed_recs)); 
					results.add(_future) ;  
				} 
			 
				for(Future<List<Record32KBytes>> one_future :results)
				{
					 try {
						List<Record32KBytes> failed_record = one_future.get();
					 } catch (InterruptedException | ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						Thread.currentThread().interrupt(); 
					 } 
				} 
			}
		}
		
		//if(this.t_conf.fulltext_searchable_columns!=null)
		if(!column_fulltext_indexer_map.isEmpty())
		{ 
			
			Iterator<String> it0 = column_fulltext_indexer_map.keySet().iterator();
			List<Future<List<Record32KBytes>>> results = new ArrayList<Future<List<Record32KBytes>>>();
			
			while(it0.hasNext())
			{
				String column = it0.next(); 
				 
				LunarLHTEngine column_indexer = column_fulltext_indexer_map.get(column);
				if(column_indexer != null)
				{	
					Future<List<Record32KBytes>> _future = writer_thread_executor
														.submit(new SETaskColumnIndexer(this, 
																/*records_for_fulltext_index, */
																succeed_recs,
																column_indexer,
																column));
					
					results.add(_future);
				}
			}
			
			for(Future<List<Record32KBytes>> one_future :results)
			{
				 try {
					List<Record32KBytes> failed_record = one_future.get();
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					Thread.currentThread().interrupt(); 
				} 
			} 	
			
			/*
			 * @Deprecated
			try { 
					this.fulltext_index_future = writer_thread_executor
													.submit(new SETaskColumnIndexer(this,  
															succeed_recs,
															column_fulltext_indexer_map));
				
					List<Record32KBytes> failed_record = this.fulltext_index_future.get();
			  
				 	
				} catch (InterruptedException | ExecutionException e) { 
					e.printStackTrace(); 
					Thread.currentThread().interrupt(); 
					System.err.println("[INTERRUPTED]: current thread "
									+ Thread.currentThread() + " @LunarTable.Insert of "
									+ this.table_name +"is interrupted."  );
						 
				}
		 
			
			*/
			/* 
			this.writer_thread_executor
						.submit(new SETaskColumnIndexer(this, 
														records_for_fulltext_index, 
														column_fulltext_indexer_map));
			 */  
			/*
			succeed = true; 
			while(!records_failed_in_fulltext_index.isEmpty())
			{
				List<Record32KBytes> failed = records_failed_in_fulltext_index.take();
				 
				// do something for these failed records
				  
				succeed =  false;
			}
			*/
		 
			//if(failed_record == null)
			//	succeed = true; 
			//else
			//	succeed =  false;
			 
		} 
		
		this.current_status = DBRuntimeStatus.onWaiting;
		critical_reference.getAndDecrement();
		 
		return succeed_recs;
	}

	/*
	 * append the column index for a given record. 
	 * the input extra_content will not be stored. 
	 * After appending, when we search keywords contained in the extra_content,
	 * the record with rec_id will be returned.
	 */
	public boolean appendFulltextIndexFor(int rec_id, 
											String column, 
											String extra_content,
											ExecutorService tokenizer_thread_executor)   {
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return false;
		
		
		/* 
		while(critical_reference.get()>0)
			;
		
		
		critical_reference.getAndIncrement();
		*/
		this.current_status = DBRuntimeStatus.onWriting;
  		 
		 
		//if(this.t_conf.fulltext_searchable_columns!=null)
		//{ 
			if(column_fulltext_indexer_map.get(column) != null)
			{
				/*
				 * use non-blocking way
				 */
				
				 tokenizer_thread_executor.submit(new SETaskColumnIndexAppender(this, 
														rec_id,
														column,
														extra_content, 
														column_fulltext_indexer_map.get(column)));
			 
			} 
			 
		//} 
		
		this.current_status = DBRuntimeStatus.onWaiting;
		/*
		critical_reference.getAndDecrement();
		 */
		
		return true;
	}

	public boolean appendFulltextIndexFor(int rec_id, String column, HashMap<String, TermScore> ter_score_map  )   {
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return false;
		
		
		 
		while(critical_reference.get()>0)
			;
		
		
		critical_reference.getAndIncrement();
		this.current_status = DBRuntimeStatus.onWriting;
  		 
		 
		//if(this.t_conf.fulltext_searchable_columns!=null)
		//{ 
			if(column_fulltext_indexer_map.get(column) != null)
			{
				try {
					column_fulltext_indexer_map.get(column).index(rec_id, ter_score_map);
				} catch (IOException e) {
					System.err.println("[ERROR]: error appending index for reccord " 
										+ rec_id + " @LunarTable.appendFulltextIndexFor(int rec_id, String column, HashMap<String, TermScore> ter_score_map)");
					e.printStackTrace();
				} 
				
				/*
				 * save the indexer, but slows down (50%) the insertion.
				 * 1 million text records cost 65~70s in inserting,
				 * with this flush per 10000 records, cost 90~95s to finish.
				 */

				try
				{
					column_fulltext_indexer_map.get(column).flushEntries();
				}
				catch (IOException e) {
					System.err.println("[Error]: failed saving "
										+ column 
										+ " indexer @LunarTable.appendFulltextIndexFor.");
								
					e.printStackTrace();
				} 
					 
			} 
			 
		//} 
		
		this.current_status = DBRuntimeStatus.onWaiting;
		critical_reference.getAndDecrement();
		 
		return true;
	}
	
	public boolean deleteRecord(int rec_id) 
	{  
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed
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
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_reference.get()>0)
			;
		
		critical_reference.getAndIncrement();
		this.current_status = DBRuntimeStatus.onDeleting;
		
		boolean succeed = false;
		Record32KBytes r_tobe_deleted = null; 
		try {
			//System.err.println("now get record " + rec_id + " for deleting.");
			r_tobe_deleted = this.record_reader.getRecord(rec_id);
			//System.err.println(" record " + rec_id + " is ready.");
			
			if(r_tobe_deleted == null)
			{
				this.current_status = DBRuntimeStatus.onWaiting;
				critical_reference.getAndDecrement(); 
				return succeed;
			}
			//System.err.println(" record " + rec_id + " is going to be deleted.");
			
			if( !record_writer.deleteRecord(rec_id))
			{
				//System.err.println(" record " + rec_id + " is deleted.");
				
				r_tobe_deleted=null;
			}
			//System.err.println(" record " + rec_id + " is going to be removed from memory.");
			 
			memory_table.releaseObj(rec_id); 
			//System.err.println(" record " + rec_id + " is removed from memory.");
			
			succeed = true;
				
		} catch (IOException e) {
				//Log this here
			succeed = false;
			e.printStackTrace();
				
		}  
		 
		/*
		if(this.real_time_mode)
		{
			if(r_tobe_deleted!=null)
			{  
				
				HashMap<String, LunarColumn> columns = r_tobe_deleted .getColumns();
				Iterator<String> keys =  columns.keySet().iterator();
				while(keys.hasNext())
				{
					LunarColumn lc = columns.get(keys.next());  
					lm_storage.delete(lc.getColumnName(), 
										lc.getColumnValue(), 
										r_tobe_deleted.getID());
				}
			}
			
		}*/
	 
		this.current_status = DBRuntimeStatus.onWaiting;
		critical_reference.getAndDecrement();
		
	 
		return succeed;
	}

	public boolean markCMDSucceed(int rec_id)
	{
		return setRecordStatus( rec_id, RecStatus.succeed); 
	}
	
	private boolean setRecordStatus(int rec_id, RecStatus _status) 
	{  
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed
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
				|| this.current_status == DBRuntimeStatus.onSettingStatus)
			return false;
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_reference.get()>0)
			;
		
		critical_reference.getAndIncrement();
		this.current_status = DBRuntimeStatus.onSettingStatus;
		
		boolean succeed = false;
		Record32KBytes r_tobe_set_status = null; 
		try { 
			r_tobe_set_status = this.record_reader.getRecord(rec_id);
			 
			if(r_tobe_set_status == null)
			{
				this.current_status = DBRuntimeStatus.onWaiting;
				critical_reference.getAndDecrement(); 
				return succeed;
			}
			//System.err.println(" record " + rec_id + " is going to be deleted.");
			
			if( !record_writer.setRecStatus(rec_id, _status) )
			{
				//System.err.println(" record " + rec_id + " is deleted.");
				
				r_tobe_set_status=null;
				succeed = false;
			}
			else 
				succeed = true;
				
		} catch (IOException e) {
				//Log this here
			succeed = false;
			e.printStackTrace();
				
		}  
	 
	 
		this.current_status = DBRuntimeStatus.onWaiting;
		critical_reference.getAndDecrement();
		
	 
		return succeed;
	}

	
	public boolean updateRecord(int rec_id, String[] columns, String[] new_val) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed
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
		
		/*
		 * only one thread can write at a time. 
		 * no concurrent writing.
		 */
		while(critical_reference.get()>0)
			;
		
		critical_reference.getAndIncrement();
		this.current_status = DBRuntimeStatus.onUpdating;
	
		if(columns.length != new_val.length)
			return false;
		
		String[] old_val = new String[new_val.length];
		
		boolean succeed = false;
		Record32KBytes r_tobe_updated = null; 
		try {
			r_tobe_updated = this.record_reader.getRecord(rec_id);
			if(r_tobe_updated == null)
			{
				critical_reference.getAndDecrement();
				return succeed; 
			}
			 
			old_val = r_tobe_updated.update(columns, new_val, old_val);
			
			r_tobe_updated = this.record_writer.updateRecord(r_tobe_updated);
			if(r_tobe_updated != null)
				succeed = true;
				
		} catch (IOException e) {
				//Log this here
			succeed = false;
			e.printStackTrace();
				
		}  
		

		this.current_status = DBRuntimeStatus.onWaiting;
		critical_reference.getAndDecrement();  
	
		return succeed;
		
		
		/* 
		if(this.real_time_mode)
		{
			if(r_tobe_updated!=null)
			{  
				for(int i=0;i<columns.length;i++)
				{  
					try {
						
						lm_persistent.updateRecord(column[i],
													new_val[i], 
													old_val[i],
													r_tobe_updated.getID(),
													r_tobe_updated.getVersion());
												
					}
					catch (IOException e) {
						//Log this here
					succeed = false;
					e.printStackTrace();
						
					} 	
				} 
			}
			
		}
		*/
		
		
	}

	/*
	 * A query is a combination of column and its value: 
	 * column,
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
	public ArrayList<Record32KBytes> query(String column, String column_value, int latest_count) throws IOException, InterruptedException, ExecutionException {
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return null;
		/*
		if( DBFSProperties.full_index_mode)
		{	 
		
			this.current_status = Status.onReading;
			Future<ArrayList<Record32KBytes>> results = this.reader_thread_executor
				.submit(new TaskSearch(property, property_value, latest_count, 
							this.record_reader,lunar_lht_engin));
	 
			this.current_status = Status.onWaiting;
			return results.get(); 
		}
		*/
		
		if(this.real_time_mode)
		{
			this.current_status = DBRuntimeStatus.onReading;
			//int[] result_ids = lunar_max.search(property, property_value)[1];
			//int[][] result = lm_persistent.getLunarMax().search(column, column_value);
			
			int count = lm_storage.rangeCount(column, column_value);
			if(count<=0)
			{
				this.current_status = DBRuntimeStatus.onWaiting;
				return null;
			}
			long[] result_keys = new long[count];
			int[] result_ids = new int[count];
			int result_count = lm_storage.search(column, 
							Long.parseLong(column_value),
							Long.parseLong(column_value),
							result_keys,
							result_ids); 
		  
			
			if(result_ids.length < latest_count || latest_count <=0)
				latest_count = result_ids.length;
			ArrayList<Record32KBytes> results = this.record_reader.getRecordsByIds(result_ids, latest_count);
			
			this.current_status = DBRuntimeStatus.onWaiting;
			return results;  
		} 
		
		return null;
	}
	
	/*
	 * A query_expression is a single keyword for full text search. 
	 * 
	 * LunarDB returns a list of records whose column "comments" 
	 * contain the keyword "buys": 
	 * {name=jackson, payment=500, comments=[\" jackson buys this phone  "]}
	 * {name=Lucy, payment=500, comments=[\" Lucy buys this phone  "]}
	 * {name=John, payment=500, comments=[\" John buys this phone  "]}
	 * {name=Rafael, payment=500, comments=[\" Rafael buys this phone  "]}
	 * ...
	 * 
	 * This interface does not support logical combination of keywords.
	 * 
	 * latest_count <=0, will return all the matching records
	 */
	public int[]  queryFullText(String column, 
								String keyword,
								int latest_count, 
								ExecutorService reader_thread_executor)  
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return null;
		
		this.current_status = DBRuntimeStatus.onReading;
		if(this.column_fulltext_indexer_map.get(column) != null)
		{
			/*
			 * Does the multi-thread really do any good to the performance?
			 * 
			 */
			/*
			Future<int[]> result_ids = reader_thread_executor
					.submit(
							new SETaskSearchIDs(column, 
									keyword, 
									latest_count, 
									this.column_fulltext_indexer_map.get(column)
									)	
							);
		 	
			this.current_status = DBRuntimeStatus.onWaiting;
			try {
				return result_ids.get();
			} catch (InterruptedException | ExecutionException e) { 
				e.printStackTrace();
				return null;
			} 
			*/
			
			SETaskSearchIDs se_task = new SETaskSearchIDs(column, 
														keyword, 
														latest_count, 
														this.column_fulltext_indexer_map.get(column)
														);
			int[] result_ids = se_task.call();
			return result_ids;
		}
		return null;
		
	}
	
	/*
	 * a statement follows the grammar of this:
	 * column against("keyword1, keyword2 + keyword3 "), 
	 * which is similar to the standard mysql fulltext query. 
	 * 
	 * LunarDB returns a list of ids 
	 * whose corresponding records have column "comments" 
	 * containing the keyword "buys + phone": 
	 * {name=jackson, payment=500, comments=[\" jackson buys this phone  "]}
	 * {name=Lucy, payment=500, comments=[\" Lucy buys this phone  "]}
	 * {name=John, payment=500, comments=[\" John buys this phone  "]}
	 * {name=Rafael, payment=500, comments=[\" Rafael buys this phone  "]}
	 * ...
	 * 
	 *  
	 */
	public HashMap<Integer, Integer> queryFullText(String statement, 
											int latest_count, 
											ExecutorService reader_thread_executor) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return null;
		
		this.current_status = DBRuntimeStatus.onReading;
		LexerSubExpression l = new LexerSubExpression(this.table_name, statement, true);
		Expression e = null;
		try {
			e = l.nextSubExpression(0);
		} catch (Exception e1) {
			System.err.println("[ERROR]: statement \"" + statement + "\" has expression error, please check it again.");
			e1.printStackTrace();
			return null;
		} 
		if(e ==null || e.getClass() != ExpressionKeywords.class)
			return null;
		
		ExpressionKeywords exp_keyword = (ExpressionKeywords)e;
		exp_keyword.setCount(0, latest_count); 
		/*
		 * <id, score>
		 */
		if(b_l_visitor == null)
			b_l_visitor = new BinaryLogicalVisitor(this, reader_thread_executor);
		
		LFuture<HashMap<Integer, Integer>> result = exp_keyword.accept(b_l_visitor);
		
		 
		//Set<Integer> ids = result.get().keySet() ; 
		  
		this.current_status = DBRuntimeStatus.onWaiting;
		//return this.record_reader.getRecordsByIds(ids, latest_count);
		 
		//return ids;
		return result.get();
		
	}
	
	 
	
	/*
	 * set top_count 0 if you want all the ids for column=value
	 */
	public int[] queryIDs(String column, String column_value, int latest_count)   {
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return null;
		
		/*
		if(DBFSProperties.full_index_mode)
		{
			this.current_status = Status.onReading;
			Future<int[]> result_ids = this.reader_thread_executor
					.submit(new TaskSearchIDs(property, property_value, latest_count, 
								 		lunar_lht_engin));
		 
			this.current_status = Status.onWaiting;
			try {
				return result_ids.get();
			} catch (InterruptedException | ExecutionException e) { 
				e.printStackTrace();
				return null;
			} 
		}
		*/
		
		if(this.real_time_mode)
		{
			this.current_status = DBRuntimeStatus.onReading;
			//int[] result_ids = lunar_max.search(property, property_value)[1];
			//int[][] result_kv = lm_persistent.getLunarMax().search(column, column_value) ;
			int count = lm_storage.rangeCount(column, column_value);
			long[] result_keys = new long[count];
			int[] result_ids = new int[count];
			int result_count = lm_storage.search(column, 
							Long.parseLong(column_value),
							Long.parseLong(column_value),
							result_keys,
							result_ids);
			
			int[] results = null;
			if(result_count >0)
			{
				if(result_ids.length < latest_count || latest_count <=0)
					results = result_ids ;
				else
				{
					results = new int[latest_count];
					System.arraycopy(result_ids, 
										result_ids.length-latest_count, 
										results, 
										0, 
										latest_count);
				}
			}
				
			this.current_status = DBRuntimeStatus.onWaiting;
			return results;  
		}
		
		return null; 
		 
	}
	
	public int[] queryRangeIDs(String column, long key_start, long key_end ) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return null;
		 
		if(this.real_time_mode)
		{
			this.current_status = DBRuntimeStatus.onReading;
			//int[][] result_keys_ids = lm_persistent.getLunarMax().search(column, key_start, key_end) ;
			int count = lm_storage.rangeCount(column, key_start, key_end);
			if(count <=0)
				return null;
			long[] result_keys = new long[count];
			int[] result_ids = new int[count];
			int result_count = lm_storage.search(column, 
							key_start,
							key_end,
							result_keys,
							result_ids);
			
			this.current_status = DBRuntimeStatus.onWaiting;
			return result_ids;  
		} 
		return null;  
	}
	/*
	public int[][] queryRangeIDs(String column, int key_start, int key_end) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return null;
		 
		if(this.real_time_mode)
		{
			this.current_status = DBRuntimeStatus.onReading;
			int[][] result_keys_ids = lm_persistent.getLunarMax().search(column, key_start, key_end) ;
			this.current_status = DBRuntimeStatus.onWaiting;
			return result_keys_ids;  
		} 
		return null;  
	}
	public int[][] queryRangeIDs(String column, float key_start, float key_end) 
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing)
			return null;
		 
		if(this.real_time_mode)
		{
			this.current_status = DBRuntimeStatus.onReading;
			int[][] result_keys_ids = lm_persistent.getLunarMax().search(column, key_start, key_end) ;
			this.current_status = DBRuntimeStatus.onWaiting;
			return result_keys_ids;  
		} 
		return null;  
	}
*/
	 
	public boolean contain(String column, String value)   
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return false;
		
		if(this.real_time_mode)
		{
			this.current_status = DBRuntimeStatus.onReading;
			//int count = lm_persistent.getLunarMax().rangeCount(column, value);
			int count = lm_storage.rangeCount(column, value);
			this.current_status = DBRuntimeStatus.onWaiting;
			return count>0?true:false;  
		} 
		return false;  
	}
	
	public DataTypes columnDataType(String column)
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return null;
		
		//DataTypes type =  this.lm_persistent.getLunarMax().getDataType(column);
		DataTypes type = lm_storage.getDataType(column);
		if(type != null)
			return type;
		else
		{
			if(this.t_conf.containStorable(column)
					|| this.t_conf.containFulltextSearchable(column))
				return DataTypes.STRING;
			else
				return null;
		}
		
	}
	public int recordsCount()
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return 0;
		return this.record_reader.recordsCount();
	}
	
	public String tableName()
	{
		return this.table_name;
	}
	
	public String tablePath()
	{
		return this.table_root_path;
	}
	
	private boolean inArray(String[] str_arr, String to_be_tested)
	{
		for(int i=0;i<str_arr.length;i++)
		{
			if(str_arr[i].equalsIgnoreCase(to_be_tested))
				return true;
		}
		return false;
	}
	
	public String[] tableColumns()
	{
		int count = 0; 
		
		String[] all_columns = null;
		if(this.t_conf.rt_searchable_columns != null
			&& this.t_conf.rt_stored_columns != null)
		{
			count = this.t_conf.rt_searchable_columns.length
					+ this.t_conf.rt_stored_columns.length;
			
			all_columns = new String[count];
			System.arraycopy(this.t_conf.rt_searchable_columns, 
								0, 
								all_columns, 
								0, 
								this.t_conf.rt_searchable_columns.length);
			System.arraycopy(this.t_conf.rt_stored_columns, 
								0, 
								all_columns, 
								this.t_conf.rt_searchable_columns.length, 
								this.t_conf.rt_stored_columns.length);
		}
		else if(this.t_conf.rt_searchable_columns == null
				&& this.t_conf.rt_stored_columns != null)
		{
			count = this.t_conf.rt_stored_columns.length;
			all_columns = new String[count];
			System.arraycopy(this.t_conf.rt_stored_columns, 
								0, 
								all_columns, 
								0, 
								this.t_conf.rt_stored_columns.length);
		}
		else if(this.t_conf.rt_searchable_columns != null
				&& this.t_conf.rt_stored_columns == null)
		{
			count = this.t_conf.rt_searchable_columns.length;
			all_columns = new String[count];
			System.arraycopy(this.t_conf.rt_searchable_columns, 
								0, 
								all_columns, 
								0, 
								this.t_conf.rt_searchable_columns.length);
		}
		
		return all_columns;
		
	}
	
	public DBRuntimeStatus getStatus()
	{
		return this.current_status;
	}
	
	 
	public ArrayList<Record32KBytes> fetchRecords(int[] record_array) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return null;
		
		return this.record_reader.getRecordsByIds(record_array);
	}
	
	public ArrayList<Record32KBytes> fetchRecords(Set<Integer> ids, int from, int count) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return  null; 
		return this.record_reader.getRecordsByIds(ids, from, count);
	}
	
	public ArrayList<Record32KBytes> fetchRecords(int[] record_array, int from, int count) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return  null;
		
		if(from + count >= record_array.length)
			count = record_array.length - from;
		return this.record_reader.getRecordsByIds(record_array, from, count);
	}
	
	
	public ArrayList<Record32KBytes> fetchLatestRecords(int latest_n) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return  null;
		
		return this.record_reader.getLatestRecords(latest_n);
	}
	
	/*
	 * by default it is DESC, i.e. the latest on the top of the result list.
	 * Example: table has 1000 records, and fetchRecords(0, 100) returns:
	 * rec 999, rec 998, rec 997, ... , rec 900
	 */
	public ArrayList<Record32KBytes> fetchRecords(int from, int count) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return  null;
		
		return this.record_reader.getRecords(from, count);
	}
	

	/*
	 * ASC, i.e. the oldest on the top of the result list.
	 * Example: table has 1000 records, and fetchRecords(0, 100) returns:
	 * rec 0, rec 1, rec 2, ... , rec 99
	 */
	public ArrayList<Record32KBytes> fetchRecordsASC(int from, int count) throws IOException
	{
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return  null;
		
		return this.record_reader.getRecordsASC(from, count);
	}
	
	
	public void save(ExecutorService writer_thread_excutor) throws IOException {
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return  ;
		
		while(critical_reference.get()>0)
			;
		
		critical_reference.getAndIncrement();
		this.current_status = DBRuntimeStatus.onWriting;
		/*
		if (lunar_lht_engin != null) 
			this.lunar_lht_engin.flush();
		 */
		 
		//if (lm_persistent != null)
		//	lm_persistent.flush();
		if (lm_storage != null)
		{
			//lm_storage.save();
			ArrayList<Future<Long>> saved_at_physical_addr = new ArrayList<Future<Long>> ();
			Iterator<String> columns = lm_storage.columnIterator(); 
			 
			while(columns.hasNext())
			{
				DBTaskSaveColumnIndex dbtsci = new DBTaskSaveColumnIndex(this , lm_storage, columns.next());
				Future<Long> _future = writer_thread_excutor.submit(dbtsci); 
				saved_at_physical_addr.add(_future);
			} 
			
		 /*
			for(Future<Long> one_future :saved_at_physical_addr)
			{
				 try {
					long addr = one_future.get();
				 } catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					Thread.currentThread().interrupt(); 
				 } 
			}*/ 
		}
		
		this.record_writer.flush();
		
		Iterator<String> it0 = column_fulltext_indexer_map.keySet().iterator();
		while(it0.hasNext())
		{
			String column = it0.next(); 
			LunarLHTEngine t_llhte = column_fulltext_indexer_map.get(column);
			t_llhte.flush(); 
		}
		this.current_status = DBRuntimeStatus.onWaiting;
		critical_reference.getAndDecrement();
	}

	public void closeTable() throws IOException {
		/*
		 * first of all, let the running threads finish their jobs, then close
		 * them.
		 */
		if(this.current_status == DBRuntimeStatus.closed
				|| this.current_status == DBRuntimeStatus.onClosing
				|| this.current_status == DBRuntimeStatus.removed)
			return;
		/*
		 * call shutdown to disable new tasks to be submitted.
		 */
		
		if(!shutdown_called.get())
			shutdown_called.set(true);
		int printed = 0;
		while(critical_reference.get()>0)
		{
			if(printed == 0)
			{
				System.out.println("waiting critical task finish. @LunarTable.closeTable()");
				printed++;
			}
		}

		
		this.current_status = DBRuntimeStatus.onClosing;
		
		System.out.println("[INFO]: LunarBase table " + this.table_name + " is shutting down now........");
		
		 
		/*
		if (this.writer_thread_executor != null)
		{	
			this.writer_thread_executor.shutdown();
			this.writer_thread_executor.shutdownNow();
		}
		
		if (this.tokenizer_thread_executor != null)
		{	
			this.tokenizer_thread_executor.shutdown();
			this.tokenizer_thread_executor.shutdownNow();
		}
		
 	 
		if (this.reader_thread_executor != null)
		{
			this.reader_thread_executor.shutdown();
			this.reader_thread_executor.shutdownNow();
		}
		
		*/
		
		
		if (this.b_l_visitor != null)
		{
			b_l_visitor.shutDown();
		}
		
		
		if(memory_table != null)
		{
			memory_table.shutDown();
			memory_table = null;
		}
		
		if (record_writer != null)
		{
			record_writer.close();
			record_writer = null;
		}
		if (record_reader != null)
		{ 
			record_reader = null;
		}
		if (this.records_queue != null)
		{
			this.records_queue.clear();
			this.records_queue = null;
		}
	 
		/*
		if (lunar_lht_engin != null)
		{
			lunar_lht_engin.close();
			lunar_lht_engin = null;
		}
		*/
		/*
		if (lm_persistent != null)
		{
			lm_persistent.close();
			lm_persistent = null;
		}*/
		if (lm_storage != null)
		{
			lm_storage.shutDown();
			lm_storage = null;
		}
		
		Iterator<String> it0 = column_fulltext_indexer_map.keySet().iterator();
		while(it0.hasNext())
		{
			String column = it0.next(); 
			LunarLHTEngine t_llhte = column_fulltext_indexer_map.get(column);
			t_llhte.close();
		}
		column_fulltext_indexer_map.clear();  
		
		t_conf.close();
		t_conf = null;
		this.current_status = DBRuntimeStatus.closed;
		System.out.println("LunarTable exited OK........");
	}

}

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

package LunarX.Node.ThreadTasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import LCG.DB.Table.LunarLHTEngine;
import LunarX.Node.API.LunarTable;
import LunarX.RecordTable.RecordHandlerCenter;
import LunarX.RecordTable.StoreUtile.LunarColumn;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
import Lunarion.SE.AtomicStructure.RowEntry;
import Lunarion.SE.AtomicStructure.TermScore;

public class SETaskColumnIndexer implements Callable<List<Record32KBytes>> {
 
	private LunarTable g_table;
	//private BlockingQueue<Record32KBytes[]> g_records_queue;
	private Record32KBytes[]  g_records_waiting;
	 
	//private HashMap<String, LunarLHTEngine> g_columns_indexer;
	private LunarLHTEngine g_columns_indexer;
	private String g_for_which_column; 

	public SETaskColumnIndexer(LunarTable _table,
							/* BlockingQueue<Record32KBytes[]> _records_waiting,  */
							Record32KBytes[] _records_waiting,
							LunarLHTEngine _lht_engin, 
							String _column) {
		this.g_table = _table;
		this.g_columns_indexer = _lht_engin;
		g_for_which_column =  _column;
		this.g_records_waiting = _records_waiting;
	 
	}

	
	@Override
	public List<Record32KBytes> call() {
	 
		List<Record32KBytes> failed_list = new ArrayList<Record32KBytes>();
		
		g_table.increaseCriticalReference();
		
			
		//while (!this.g_records_queue.isEmpty()) {
			int i = 0;
			 
			//try 
			//{
				//Record32KBytes[] records = this.g_records_queue.take();
				//Record32KBytes[] records = this.g_records_waiting;
				

				//long start_time = System.nanoTime();   
				
				
				for( i =0; (i<g_records_waiting.length && g_records_waiting[i].getID() > -1);i++)
				{  
					//Iterator<String> it0 = g_columns_indexer.keySet().iterator();
					//while(it0.hasNext())
					//{
						//String column = it0.next(); 
						try
						{
							//g_columns_indexer.get(column).index(g_records_waiting[i].getID(),g_records_waiting[i].getColumn(column));
							g_columns_indexer.index(g_records_waiting[i].getID(),g_records_waiting[i].getColumn(g_for_which_column));
							
						
						}catch (IOException e) {
							System.err.println("[Error]: failed indexing record "
								+ g_records_waiting[i] + "at column " +  g_for_which_column
								+ " of table " 
								+ g_table
								+ " @SETaskColumnFullTextIndexer.");
							failed_list.add(g_records_waiting[i]);
							e.printStackTrace();
						} 
					//}
				}
				
				 //long end_time = System.nanoTime();
				 
				// long duration_each = end_time - start_time;
				 
				 //System.err.println("@" + Thread.currentThread() + ", indexing " + records.length + " records has taken: " + duration_each/1000000 + " ms");  
				
				
				/*
				 * save the indexer, but slows down (50%) the insertion.
				 * 1 million text records cost 65~70s in inserting,
				 * with this flush per 10000 records, cost 90~95s to finish.
				 */
			 
				//Iterator<String> it1 = g_columns_indexer.keySet().iterator();
				//while(it1.hasNext())
				//{
					//String column = it1.next();  
					try {
						//g_columns_indexer.get(column).flushEntries();
						g_columns_indexer.flushEntries();
						
					}catch (IOException e) {
							System.err.println("[Error]: failed saving "
									+ g_for_which_column 
									+ " indexer @SETaskColumnFullTextIndexer.");
							
							e.printStackTrace();
					} 
				//}  
			//}
				
			//catch (InterruptedException ie) {  
			//	Thread.currentThread().interrupt(); 
			//	System.err.println("[INTERRUPTED]: @"
			//			+ Thread.currentThread()  );
				 
			//}  
			
		//}
		g_table.decreaseCriticalReference();
		//System.err.println("[XXXXXXXX]: current thread "
		//		+ Thread.currentThread() + " is interrupted?" + Thread.currentThread().isInterrupted() );
	 
		if(failed_list.size() == 0)
			return null;
		
		return failed_list;
		 
	 
	}
	
}
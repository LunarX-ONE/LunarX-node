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
import LunarX.RecordTable.RecordHandlerCenter;
import LunarX.RecordTable.StoreUtile.LunarColumn;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
import Lunarion.SE.AtomicStructure.RowEntry;
import Lunarion.SE.AtomicStructure.TermScore;

/*
 * this implementation submit a task that keeps running even no data 
 * in the queue.
 * 
 * It is with low efficient. It is terribly racing resources with other 
 * threads.
 */
public class SETaskColumnIndexerVOld implements Runnable {
 
	 
	private BlockingQueue<Record32KBytes[]> g_records_queue;
	private BlockingQueue<List<Record32KBytes>> g_failed_records_queue;
	 
	private HashMap<String, LunarLHTEngine> g_columns_indexer;
	 

	public SETaskColumnIndexerVOld(BlockingQueue<Record32KBytes[]> _records_waiting,
							BlockingQueue<List<Record32KBytes>> _records_failed,
							HashMap<String, LunarLHTEngine> _lht_engin) {
		this.g_columns_indexer = _lht_engin;
		//this.g_record_writer = _record_writer;

		this.g_records_queue = _records_waiting;
		this.g_failed_records_queue = _records_failed;
	}

	
	@Override
	public void run() {
	 for(;;)
	 {
		List<Record32KBytes> failed_list = new ArrayList<Record32KBytes>();
		
		while (!this.g_records_queue.isEmpty()) {
			try {
				Record32KBytes[] records = this.g_records_queue.take();
				 
				for(int i =0; i<records.length;i++)
				{
					Iterator<String> it0 = g_columns_indexer.keySet().iterator();
					while(it0.hasNext())
					{
						String column = it0.next();  
						try {
							g_columns_indexer.get(column).index(records[i].getID(),records[i].getColumn(column));
							}catch (IOException e) {
								System.err.println("[Error]: failed indexing column @TaskColumnFullTextIndexer.");
								failed_list.add(records[i]);
								e.printStackTrace();
							} 
						}
					} 
				}
				
			catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}  
		}
		if(failed_list.size()>0)
		{
			try {
				g_failed_records_queue.put(failed_list);
			} catch (InterruptedException e) {
				 
				e.printStackTrace();
			}
		}
		
	 }
	 
	}


	 
	
}
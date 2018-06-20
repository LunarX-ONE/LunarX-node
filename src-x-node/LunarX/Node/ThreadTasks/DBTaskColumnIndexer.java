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
import LCG.MemoryIndex.LunarMaxWithStorage;
import LunarX.Node.API.LunarTable;
import LunarX.RecordTable.RecordHandlerCenter;
import LunarX.RecordTable.StoreUtile.LunarColumn;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
import Lunarion.SE.AtomicStructure.RowEntry;
import Lunarion.SE.AtomicStructure.TermScore;

public class DBTaskColumnIndexer implements Callable<List<Record32KBytes>> {
 
	private LunarTable g_table;
	private Record32KBytes[] g_recs;
	private LunarMaxWithStorage g_lm_storage;
	private String g_for_which_column;
	 
	 
	public DBTaskColumnIndexer(LunarTable _table,
							LunarMaxWithStorage _lm_storage,
							String _column,
							Record32KBytes[]  _records_waiting ) {
		this.g_table = _table;
		g_lm_storage = _lm_storage;  
		g_for_which_column = _column;
		this.g_recs = _records_waiting;
		
	}

	
	@Override
	public List<Record32KBytes> call() {
	 
		List<Record32KBytes> failed_list = new ArrayList<Record32KBytes>();
		g_table.increaseCriticalReference();
		int k=0; 
		 
		//g_table.insertLunarMax(g_recs );
		for(k=0; k<g_recs.length;k++) 
			insertLunarMax(g_recs[k]);
			 
		/*
		 * 5 times slow down the indexing performance, 
		 * therefore we do not suggest to save the column index every time of insert. 
		 */
		//g_lm_storage.saveColumn(g_for_which_column);
			 
		 //g_lm_storage.save(); 
			
		 
		g_table.decreaseCriticalReference();
		if(failed_list.size() == 0)
			return null;
		return failed_list; 
	}
	
	private void insertLunarMax(Record32KBytes rec )  
	{
		if(rec.getID() > -1)
		{
			/*
			String[] p_v_pairs = rec .getPVPairs();
			for(int i=0;i<p_v_pairs.length;i++)
			{
				String[] pv = p_v_pairs[i].trim().split("=");
				lm_persistent.insertRecord(pv[0].trim(), pv[1].trim(), 
										rec.getID(), 
										rec.getVersion()); 
			} 
			*/
			 
			//Iterator<String> columns = g_lm_storage.columnIterator();
			//while(columns.hasNext())
			//{
				//String col = columns.next();
				LunarColumn lc = rec.getColumn(g_for_which_column);
				if(lc != null)
				{ 
					g_lm_storage.insertKey(g_for_which_column, lc.getColumnValue(), rec.getID());
				}
			//} 
		} 
	}
	
}
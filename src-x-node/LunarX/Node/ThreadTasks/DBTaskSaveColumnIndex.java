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

public class DBTaskSaveColumnIndex implements Callable<Long> {
 
	private LunarTable g_table;
	private LunarMaxWithStorage g_lm_storage;
	private String g_for_which_column;
	 
	 
	public DBTaskSaveColumnIndex(LunarTable _table,
							LunarMaxWithStorage _lm_storage,
							String _column ) {
		this.g_table = _table;
		g_lm_storage = _lm_storage;  
		g_for_which_column = _column;
		 
	}

	
	@Override
	public Long call() {
	 
		g_table.increaseCriticalReference();
		 	 
		long addr = g_lm_storage.saveColumn(g_for_which_column);
		  
		g_table.decreaseCriticalReference();
		
		return addr;
		 
		 
	}
	 
}
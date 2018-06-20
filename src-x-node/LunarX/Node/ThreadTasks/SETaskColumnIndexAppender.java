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

public class SETaskColumnIndexAppender implements Callable<boolean[]>  {
 
	private LunarTable g_table;
	
	private String g_column;
	private int rec_id_that_is_indexed_with_extra_content;
	//private HashMap<String, TermScore>  ter_score_map = null;
	private String g_extra_content = null;
	  
	private LunarLHTEngine g_columns_indexer;
	 

	public SETaskColumnIndexAppender(LunarTable _table,
							int _rec_id,  
							String _column,
							//HashMap<String, TermScore> _ter_score_map  ,
							String extra_content,
							LunarLHTEngine  _lht_engin) {
		this.g_table = _table;
		this.g_columns_indexer = _lht_engin;
		 
		rec_id_that_is_indexed_with_extra_content = _rec_id;
		g_column = _column;
		//ter_score_map = _ter_score_map;
		g_extra_content = extra_content;
	}

	
	@Override
	public boolean[] call() {

		 
		HashMap<String, TermScore> ter_score_map  
					= g_columns_indexer.getTokenizer().tokenizeTerm(g_extra_content); 
	
		g_table.appendFulltextIndexFor(rec_id_that_is_indexed_with_extra_content, 
										g_column,
										ter_score_map);
			 	 
		 
		return null; 
	 
	}
	
}
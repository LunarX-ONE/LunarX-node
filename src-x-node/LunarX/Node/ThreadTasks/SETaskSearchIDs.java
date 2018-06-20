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
import java.util.List; 
import java.util.concurrent.Callable;

import LCG.DB.Table.LunarLHTEngine;
import LunarX.RecordTable.TableReader;
import Lunarion.SE.AtomicStructure.RowEntry;

public class SETaskSearchIDs implements Callable<int[]> { 
	 
	private LunarLHTEngine g_lunar_lht_engine; 

	private String g_column;
	private String g_value;
	private int g_top_count; 

	public SETaskSearchIDs(String column, String value, int top_count, 
			 LunarLHTEngine _lht_engin) {
		this.g_lunar_lht_engine = _lht_engin; 

		this.g_column = column;
		this.g_value = value.trim().toLowerCase();
		this.g_top_count = top_count; 
	}

	
	@Override
	public int[] call() { 
		 
		RowEntry r_e = null;
		try {
			r_e = g_lunar_lht_engine.readRowEntry(g_value,g_value); 

			if (r_e != null) {
				int record_count = 0;
				if(g_top_count <= 0)
					record_count = r_e.getRecCount();
				else
					record_count = Math.min(r_e.getRecCount(), g_top_count);

				int[] data_result = new int[record_count]; 
			 
				//r_e.getVFInst().readData(data_result);
				r_e.readData(data_result);
				 
				return data_result;

			}
			return null;	 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		 
		 
	}
}
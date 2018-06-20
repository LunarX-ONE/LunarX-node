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
import LunarX.Node.Utils.QuickSort;
import LunarX.RecordTable.TableReader;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
import Lunarion.SE.AtomicStructure.RowEntry;

public class TaskSort implements Callable<int[]> { 
 
	 
	private int[] ids; 
	private boolean reverse;
	private QuickSort sorter;
	
	public TaskSort(int[] _ids, boolean _reverse, QuickSort _sorter) {
		this.ids = _ids;
		this.reverse = _reverse; 
		this.sorter = _sorter; 
	} 
	
	@Override
	public int[] call() {
		 sorter.quickSort(ids, ids.length);
		 return ids;
	}
}
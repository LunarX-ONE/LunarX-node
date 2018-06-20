
/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contacts: 
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
package LunarX.Node.API.Result;

import java.io.IOException;
import java.util.ArrayList;

import LunarX.Node.API.LunarXNode;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class PTQueryResult {

	private LunarXNode db_inst;
	private String table; 
	private String key;
	
	private int[] ids;
	
	/*
	 * Point query result
	 */
	PTQueryResult(LunarXNode _db_inst, String _table, String _key, int[] _ids)
	{
		db_inst = _db_inst;
		table = _table;
		key = _key;
		ids = _ids;
	}
	
	ArrayList<Record32KBytes> fetchRecords(int top_n) throws IOException
	{
		return this.db_inst.fetchRecords(table, ids, ids.length-top_n, top_n);
	}
	
	public ArrayList<Record32KBytes> fetchRecords( int from, int count) throws IOException
	{
		return this.db_inst.fetchRecords(table, ids, from, count);
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

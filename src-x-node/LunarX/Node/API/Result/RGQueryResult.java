
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
import java.util.Iterator;
import java.util.Set;

import LunarX.Node.API.LunarXNode;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class RGQueryResult {

	private LunarXNode db_inst;
	private String table; 
	private String statement;
	
	private long[] keys;
	private int[] id_array;
	private Set<Integer> id_set;
	
	/*
	 * range query result
	 */
	@Deprecated
	public RGQueryResult(LunarXNode _db_inst, String _table, String _statement, 
						long[] _keys,
						int[] _ids)
	{
		db_inst = _db_inst;
		table = _table;
		statement = _statement;
		keys = _keys;
		id_array = _ids;
		id_set = null;
	}
	
	/*
	public RGQueryResult(LunarDB _db_inst, String _table, String _statement, 
						long[] _keys,
						Set<Integer> _id_set)
	{
		db_inst = _db_inst;
		table = _table;
		statement = _statement;
		keys = _keys;
		id_array = null;
		id_set = _id_set;
	}*/
	
	public int resultCount()
	{
		if(id_array!=null)
			return id_array.length;
		else
		{
			if(id_set == null)
				return 0;
			else
				return id_set.size();
		} 
	}
	public ArrayList<Record32KBytes> fetchRecords(int top_n) throws IOException
	{
		if(id_array!=null)
		{
			int from = (id_array.length-top_n)<0?0:(id_array.length-top_n);
			int count = (id_array.length-top_n)<0?id_array.length:top_n ;
			return this.db_inst.fetchRecords(table, id_array, from, count);
		}
		else
		{	
			return this.db_inst.getTable(table)
							.fetchRecords(id_set, 0, top_n) ;
		}
	}
	
	public ArrayList<Record32KBytes> fetchRecords() throws IOException
	{
		if(id_array!=null)
			return this.db_inst.fetchRecords(table, id_array, 0, id_array.length);
		else
			return this.db_inst.getTable(table)
							.fetchRecords(id_set, 0, id_set.size() ) ;
	}
	
	public ArrayList<Record32KBytes> fetchRecords( int from, int count) throws IOException
	{
		if(id_array!=null)
			return this.db_inst.fetchRecords(table, id_array, from, count);
		else
			return this.db_inst.getTable(table)
					.fetchRecords(id_set, from, count) ;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

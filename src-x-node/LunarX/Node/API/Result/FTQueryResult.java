
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import LunarX.Node.API.LunarXNode;
import LunarX.Node.SetUtile.SetOperation;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class FTQueryResult {

	private LunarXNode db_inst;
	private String table; 
	//private String statement;
	
	private final SetOperation set_operation = new SetOperation();
	
	private int[] id_array;
	//private Set<Integer> id_set;
	
	/*
	 * Full-text query result
	 */
	public FTQueryResult(LunarXNode _db_inst, String _table,  int[] _ids)
	{
		db_inst = _db_inst;
		table = _table;
		//statement = _statement;
		//id_array = _ids; 
		//id_set = null;
		if(_ids == null)
			id_array = null;
		else
		{
			if(_ids.length == 0)
				id_array = null;
			else
			{ 
				id_array = _ids;
			}
			
		}
		
	}
	
	//public FTQueryResult(LunarDB _db_inst, String _table, String _statement, Set<Integer> _id_set)
	public FTQueryResult(LunarXNode _db_inst, String _table,  
						HashMap<Integer, Integer> _id_map,
						int _top_n)
	{
		db_inst = _db_inst;
		table = _table;
		//statement = _statement;
		id_array = null;
		int top_n = _top_n;
		if(_id_map != null)
		{ 
			
			//Object[] key = _id_map.keySet().toArray();
			Iterator<Entry<Integer,Integer>> id_score_iter = _id_map.entrySet().iterator();
			if(id_score_iter.hasNext())
			{
				/*
				 * ArrayList<Entry<Integer,Integer>> result_to_be_sorted = new ArrayList<Entry<Integer,Integer>>();
				 * while(id_score_iter.hasNext())
					{
						result_to_be_sorted.add(id_score_iter.next());
					}
				 */
				
				ArrayList<Entry<Integer,Integer>> results_sorted
								= new ArrayList<Entry<Integer,Integer>>(_id_map.entrySet());
				
				Collections.sort(results_sorted, new Comparator<Entry<Integer,Integer>>(){
					  @Override
					  public int compare(Entry<Integer,Integer> rec_1, Entry<Integer,Integer> rec_2)
					  { 
						  if(rec_1.getValue() <  rec_2.getValue()) 
							  return -1;
						  if(rec_1.getValue() ==  rec_2.getValue())
						  {
							  if(rec_1.getKey() < rec_2.getKey())
								  return -1;
							  if(rec_1.getKey() == rec_2.getKey())
								  return 0;
							  else
								  return 1;
						  }
						  else
							  return 1;
					  }
				  });
				
				 
				if( top_n <=0)
					top_n = results_sorted.size() ;
				else
					top_n = Math.min(top_n, results_sorted.size());
				id_array = new int[top_n];
				for(int i=0;i<top_n;i++)
				{
					id_array[i] = results_sorted.get(i).getKey();
				} 
			} 
		} 
	}
	
	public int resultCount()
	{
		if(id_array!=null)
			return id_array.length;
		else
		{
			/*
			if(id_set == null)
				return 0;
			else
				return id_set.size();
				*/
			return 0;
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
			/*
			return this.db_inst.getTable(table)
							.fetchRecords(id_set, 0, top_n) ;*/
			return new ArrayList<Record32KBytes>(); 
		}
	}
	
	public ArrayList<Record32KBytes> fetchRecords() throws IOException
	{
		if(id_array!=null)
			return this.db_inst.fetchRecords(table, id_array, 0, id_array.length);
		else
		{
			/*
			return this.db_inst.getTable(table)
							.fetchRecords(id_set, 0, id_set.size() ) ;
							*/
			return new ArrayList<Record32KBytes>(); 
		}
	}
	
	public ArrayList<Record32KBytes> fetchRecords( int from, int count) throws IOException
	{
		if(id_array!=null)
		{
			//int from__ = id_array.length-from-count;
			return this.db_inst.fetchRecords(table, id_array, from, count);
		}
		else
		{
			/*
			 * return this.db_inst.getTable(table).fetchRecords(id_set, from, count) ;
			 */
			return new ArrayList<Record32KBytes>(); 
		}
	}
	
	public int[] getRecIDs()
	{
		return this.id_array;
	}
	
	public String getTableName()
	{
		return this.table;
	}
	
	public FTQueryResult unionAnother(FTQueryResult another_result)
	{
		if(this.table.equalsIgnoreCase(another_result.getTableName()))
		{
			HashMap<Integer, Integer> union_result = set_operation.union(id_array, another_result.getRecIDs());
			return new FTQueryResult(this.db_inst, this.table, union_result, 0);
		}
		else
			return null;
		
	}
	
	public FTQueryResult intersectAnother(FTQueryResult another_result)
	{
		if(this.table.equalsIgnoreCase(another_result.getTableName()))
		{
			HashMap<Integer, Integer> intersect_result = set_operation.intersectSets(id_array, another_result.getRecIDs());
			return new FTQueryResult(this.db_inst, this.table, intersect_result, 0);
		}
		return null;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

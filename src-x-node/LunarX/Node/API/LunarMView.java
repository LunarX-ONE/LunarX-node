/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author DADA database team, feiben@lunarion.com
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

package LunarX.Node.API;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException; 
import java.util.ArrayList; 
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import LCG.DB.Table.LunarLHTEngine;
import LCG.FSystem.Def.DBFSProperties;
import LCG.MemoryIndex.IndexTypes.DataTypes;
import LCG.MemoryIndex.IndexTypes;
import LCG.Utility.StrLegitimate;
import LunarX.Memory.TableMemoryStore;
import LunarX.Node.API.XNodeStatus.DBRuntimeStatus;
import LunarX.Node.Conf.EnginPathDefinition;
import LunarX.Node.ThreadTasks.SETaskColumnIndexer;
import LunarX.Node.ThreadTasks.SETaskSearchIDs;
import LunarX.Realtime.Column.LunarMaxPersistent;
import LunarX.RecordTable.RecordHandlerCenter;
import LunarX.RecordTable.TableReader;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class LunarMView extends LunarTable { 
	
	
	String view_name;
	/*
	 * table, columns
	 */
	HashMap<String, HashMap<String, DataTypes>> table_column_map;
	
	public LunarMView() {
		super();
	} 
	
	public boolean openTable(String _table_path,
			boolean _real_time_mode, 
			int _max_recs_in_memory 
			)  
	{
		return false;
	}
	public boolean openView(String _table_path,
								boolean _real_time_mode, 
								int _max_recs_in_memory, 
								DBFSProperties dbfs_prop_instance) throws IOException {
		boolean succeed = false;
		succeed = super.openTable(_table_path, 
					_real_time_mode, 
					_max_recs_in_memory,
					dbfs_prop_instance);
		view_name = this.tableName();
		table_column_map = new HashMap<String, HashMap<String, DataTypes>>();
		return succeed;
	} 
	/*
	 * (non-Javadoc)
	 * @see LCG.DB.API.LunarTable#addSearchable(java.lang.String, java.lang.String)
	 * 
	 * overwrite parent's function to avoid error settings.
	 */
	public boolean addSearchable(String data_type, String column)
	{
		return false;
	}
	public boolean addSearchable(String data_type, String table, String column) throws IOException
	{
		boolean succeed = false;
		String column_in_view = makeColumn(table, column);
		
		succeed = t_conf.addSearchable(  data_type, this.view_name, column_in_view); 
		
		DataTypes d_type =  IndexTypes.getDatatype(data_type);
		//succeed = lm_persistent.registerPersisitent (this.view_name, 
		//									column_in_view, 
		//									d_type);
		succeed = lm_storage.registerIndexer( this.view_name, 
											column_in_view, 
													d_type);
		if(table_column_map.get(table) == null)
		{
			HashMap<String, DataTypes> column_map = new HashMap<String, DataTypes>();
			table_column_map.put(table, column_map);
		}
		 
		if(table_column_map.get(table).get(column) == null 
					|| table_column_map.get(table).get(column) == DataTypes.UNKNOWN)
			table_column_map.get(table).put(column,d_type);
		else
			return false;
		 
		return succeed;
	}
	
	public boolean addStorable( String table, String column) throws IOException
	{
		boolean succeed = false;
		String column_in_view = makeColumn(table, column);
		succeed = t_conf.addStorable( this.view_name, column_in_view);  
 		 
		if(table_column_map.get(table) == null)
		{
			HashMap<String, DataTypes> column_map = new HashMap<String, DataTypes>();
			table_column_map.put(table, column_map);
		}
		 
		if(table_column_map.get(table).get(column) == null )
			table_column_map.get(table).put(column,DataTypes.UNKNOWN);
		else
			return false;
		
		return succeed;
	}
	
	public String tableName()
	{
		return super.tableName();
	}
	
	public String mviewName()
	{
		return view_name;
	}
	
	public String mviewPath()
	{
		return table_root_path;
	}
	
	public String makeColumn(String table, String column)
	{
		return table+"_"+column;
	}
	public String makeJoinRecords(String table_1, 
									Record32KBytes record_1, 
									String table_2, 
									Record32KBytes record_2)
	{
		if(this.table_column_map.get(table_1) == null
				|| this.table_column_map.get(table_2) == null)
		{
			System.err.println("LunarMView Error: "
									+ table_1 
									+ "/" + table_2 
									+ " has no column to JOIN");
			return null;
		}
		
		StringBuffer joined_record = new StringBuffer();
		
		joined_record.append("{");
		HashMap<String, DataTypes> table_1_map = this.table_column_map.get(table_1);
		Iterator<String> it1 = table_1_map.keySet().iterator();
		while(it1.hasNext())
		{
			String i_column = it1.next();  
			String val = record_1.valueOf(i_column);
			if(val != "")
				joined_record.append(makeColumn(table_1,i_column)+"="+val+",");
		}
		
		HashMap<String, DataTypes> table_2_map = this.table_column_map.get(table_2);
		Iterator<String> it2 = table_2_map.keySet().iterator();
		while(it2.hasNext())
		{
			String i_column = it2.next(); 
			String val = record_2.valueOf(i_column);
			if(val != "")
				joined_record.append(makeColumn(table_2,i_column)+"="+val+",");
		}
		
		/*
		 * remove the last ","
		 */
		if(joined_record.length()>1)
			return joined_record.replace(joined_record.length()-1, 
										joined_record.length(), 
										"}").toString();
		else
			return "";
	} 
		
	public void closeTable()
	{
		
	}
	
	public void closeView() throws IOException {
		 
		super.closeTable();
	}

}

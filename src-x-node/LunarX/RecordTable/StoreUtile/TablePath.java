package LunarX.RecordTable.StoreUtile;

import java.io.IOException;

import LCG.FSystem.Def.DBFSProperties;
import LunarX.Node.Conf.EnginPathDefinition;

public class TablePath { 
	 
	private final String record_table_path ;  
	
	private final String materialized_view_path ;
	
	private final String table_file  ;
	private final String table_index_file ;

	/*
	 * actually it is a shard of data:
	 * 
	 * table_name.vnode.0
	 * table_name.vnode.1
	 * ...
	 *
	 * table_name.removed.2
	 */
	private final String table_virtual_node_middle_name = ".vnode.";
	private final String table_virtual_node_removed_middle_name = ".removed.";
	
	private final String record_table_vnode_path ;
	
	
	public TablePath(DBFSProperties _dbfs_prop_instance)
	{
		record_table_path = 
				_dbfs_prop_instance.db_root_path 
				+ _dbfs_prop_instance.db_name 
				+ "/" 
				+ EnginPathDefinition.table_space_dir; 


		materialized_view_path = 
				_dbfs_prop_instance.db_root_path 
				+ _dbfs_prop_instance.db_name  
				+ "/" 
				+ EnginPathDefinition.materialized_view_dir;

		table_file = record_table_path + EnginPathDefinition.record_storage;
		table_index_file = record_table_path + EnginPathDefinition.record_index;  

		record_table_vnode_path = 
				_dbfs_prop_instance.db_root_path 
				/* + _dbfs_prop_instance.db_name + "/"*/ 
				+ EnginPathDefinition.table_space_dir.substring(0, 
				EnginPathDefinition.table_space_dir.length()-1)
				+ table_virtual_node_middle_name;

	}
	public String getTablePath() {
		return record_table_path;
	} 
	
	public String getVNodeTablePath()
	{
		return record_table_vnode_path;
	}
	public String getViewPath() {
		return materialized_view_path;
	} 

	public String getVNodeMiddleName()
	{
		return table_virtual_node_middle_name;
	}
	
	/*
	 * input: table_name.vnode.5
	 * output: table_name.removed.5
	 */
	public String removeVirtualNode(String table_full_name)
	{
		String[] segs = table_full_name.split("\\.");
		 
		if(!segs[segs.length-2].equals(
				table_virtual_node_middle_name.substring(
						1,table_virtual_node_middle_name.length()-1)
					)
				)
			return "";
		return table_full_name.replace(table_virtual_node_middle_name, 
								table_virtual_node_removed_middle_name);
	}
	public static String getTableFile(String table_path) {
		//return table_file;
		return table_path + EnginPathDefinition.record_storage;
	}

	public static String getTableIndexFile(String table_path) {
		//return table_index_file;
		return table_path + EnginPathDefinition.record_index;
	}
	
	public static void main(String[] args) throws IOException {
		
		DBFSProperties inst =  new DBFSProperties("/home/feiben/DBTest/RTSeventhDB/", "runtime.conf");
		TablePath table = new TablePath(inst);
		System.out.println(table.removeVirtualNode("table_name.vnode.100"));
		
	}
}

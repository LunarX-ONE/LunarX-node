/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author LunarBase team 
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

package LunarX.Node.Conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Enumeration;
import java.util.Properties;

import LCG.FSystem.Def.DBFSProperties;

public class EnginPathDefinition {

	/*
	 * following paths are actually: root_path/ + followings e.g. root_path
	 * ="/usr/data/database1", then the rowkey_hash_storage is
	 * "/usr/data/database1/rowkey_hash.store"
	 * 
	 */

	/*
	 * Names following are retained
	 */
	/*
	 * HashStorage files,
	 */
	static public final String rowkey_hash_storage = "rowkey_hash.store";
	static public final String rowkey_hash_storage_head = "rowkey_hash.head";
	static public final String row_entry_storage = "row_entry.store";
	 
	static public final String row_entry_address_dir = "row_entry_addr.dir";
	/*
	 * diction for small data set, like "name", "time_stamp" which is loaded
	 * into memory when db starts.
	 */
	static public final String property_dict = "property.dict";

	static public final String table_space_dir = "tableSpace/"; 
	static public final String materialized_view_dir = "materializedView/";
	static public final String record_storage = ".records.store";
	static public final String record_index = ".rec_index.store";
	static public final String record_table_rt_conf = ".rt.conf";

	/*
	 * bloom filters file
	 */
	static public final String filter_file = "filter.store";

	static public final String log_path = "changes.log";

 

	// e.g. root_path ="/usr/data/database1"
	// the write_locker_file is "/usr/data/database1/write.lck"
	static public String write_locker_file = "/write.lck";

	/*
	 * Runtime tunable. After changing the configure file, reload the database
	 */
 

	static public int get_data_result_count = 500;
	static public int get_data_task_count = 500;

}

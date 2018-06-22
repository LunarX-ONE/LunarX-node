/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contactor: 
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
 

package LCG.FSystem.Def;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Enumeration;
import java.util.Properties;

import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.MemoryIndex.IndexTypes.DataTypes;
import LCG.Utility.StrLegitimate;

 
public class DBFSProperties { 
	/***********
	 * 
	@Deprecated
	public static int block_64b = 1 << 6; //64b block
	public static int block_512b = 1 << 9; //512b block
	public static int block_4k = 1 << 12; //4k block
	public static int block_32k = block_4k << step; //32k block
	public static int block_256k = block_32k << step; //256k block
	public static int block_2M = block_256k << step; //2048k = 2M block 
	public static int block_16M = block_2M << step; //16M block 
	************/ 
	public DBFSProperties(String _root_path)
	{
		 
		db_root_path = _root_path.trim(); 
		if(!db_root_path.endsWith("/"))
			db_root_path = db_root_path + "/";  
	}
	
	public DBFSProperties(String _root_path, String _conf)
	{
		db_root_path = _root_path.trim(); 
		if(!db_root_path.endsWith("/"))
			db_root_path = db_root_path + "/"; 
		 
		loadCriticalValues(_root_path + _conf);
		loadConfig(_root_path + _conf);
	}
	/*
	 * internal constants, never change.
	 */
	public static int start_physical_level = 1;
	//public static int max_level = Math.min(2, BlockSizes.values().length-1);
	public int max_level_count = BlockSizes.values().length-start_physical_level;
		
	public enum BlockSizes
	{ 
		block_64b 
			{
				public int getSize(){return 1 << 6;} 
				public int getBitLen(){return 6;}
				public int getLevel(){ return 0 -start_physical_level ; }
			
			},//64b block
		block_512b 
			{
				public int getSize(){return 1 << 9;} 
				public int getBitLen(){return 9;}
				public int getLevel(){return 1 -start_physical_level ;}
			},//512b block
		block_4k 
			{
				public int getSize(){return 1 << 12;} 
				public int getBitLen(){return 12;}
				public int getLevel(){return 2 -start_physical_level ;}
			},//4k block
		block_32k 
			{
				public int getSize(){return 1 << 15;} 
				public int getBitLen(){return 15;}
				public int getLevel(){return 3 -start_physical_level ;}
			},//32k block
		block_256k 
			{
				public int getSize(){return 1 << 18;} 
				public int getBitLen(){return 18;}
				public int getLevel(){return 4 -start_physical_level ;}
			},//256k block
		block_2M 
			{
				public int getSize(){return 1 << 21;} 
				public int getBitLen(){return 21;}
				public int getLevel(){return 5 -start_physical_level ;}
			},//2048k = 2M block 
		block_16M 
		{
				public int getSize(){return 1 << 24;} 
				public int getBitLen(){return 24;}
				public int getLevel(){return 6 -start_physical_level ;}
		}; //16M block
		public abstract int getSize();
		public abstract int getBitLen(); 
		public abstract int getLevel(); 
	}; 
	/*
	public static BlockSizes getBlockByteSize(int level)
	{
		switch(level) 
		{
		case 0:
			return BlockSizes.block_64b;
		case 1:
			return BlockSizes.block_512b;
		case 2:
			return BlockSizes.block_4k;
		case 3:
			return BlockSizes.block_32k; 
		case 4:
			return BlockSizes.block_256k;
		case 5:
			return BlockSizes.block_2M;
		case 6:
			return BlockSizes.block_16M;
			
		}
		return null;
	};
	*/
	public static final Byte available = 'A';//=65, ASCII code, this block is available
	public static final Byte unavailable = 'U';//=85, ASCII code, this block has been used
	
	
	/*
	 * Default constants before building a virtual file system. 
	 * Once built, never changes.
	 * 
	 */ 
	
	public int max_4k_blocks_in_seg = 1 << 6 ;//<< 10; //65536 blocks at most
	public int seg_file_size = max_4k_blocks_in_seg * BlockSizes.block_4k.getSize(); 
	 
	 
	 
	public long records_table_file_size = ((long)1) << 18L;
	
	/*
	 * Runtime tunable.
	 */
	public int max_4k_blocks_in_cache = 1 << 4 ;//<< 10; // 16384 blocks in memory 
	public long mem_cache_size = ((long)(max_4k_blocks_in_cache) * BlockSizes.block_4k.getSize());//memory store has default 128M at most
	public int max_cache_keys_in_memory = 1 << 4;
	public int max_records_in_memory = 1<<10;
	
	//public static int disk_cache_size = Integer.MAX_VALUE >> 1;
	
	 
	//the concurrency level depends on the number of cores of CPU,
	//and more threads do no good for performance with lesser CPU cores.
	public int concurrent_level = 16; 
	public boolean native_mode = true;
	public int bit_buff_len = 15;
 
	public boolean search_engine_mode = true;
	public boolean real_time_mode = true;
	/*
	 * moved to LunarTable object
	 */
	//public static DataTypes[] rt_analysable_data_types = null;  
	//public static String[] rt_analysable_properties = null;  
	//public static String[] rt_tables = null;  
	public int rt_index_order = 7;
	public boolean rt_virtual_mem_enabled = true;
	public long rt_max_memory = 1 << 25;
	public int rt_max_virtual_pte = 4096;
	public String rt_vm_swap =  null;
	public int rt_precision = 3;
	public int rt_threads = 8;
	/*
	 * db server properties
	 */
	
	public int port = 30860;
	public String db_root_path = null;
	public String db_name = null;
	public static final String critical_prop = "critical.property";
	public static final String runtime_conf = "runtime.conf";
	/*
	 * manifold structure file directory and files for big table
	 */
	static public final String manifold_dir = "manifold"; 
	
	/*
	 * Some constant utilities.
	 */
	public static Byte getAvailability(int b)
	{
		if(b>=65 && b < 65+BlockSizes.values().length)
			return available;
		if(b>=85 && b < 85+BlockSizes.values().length)
			return unavailable;
		return -1;
	} 
	 
	public static BlockSizes getBlockSize(int s_status)
	{
		int i = s_status;		
		int level = -1; 
		if(i>=0 && i<BlockSizes.values().length)
		{
			level = i;
		}
		//unavailable
		else if(i>=85 && i<85+BlockSizes.values().length)
		{
			level = i-85; 
		}
		//available
		else if(i>=65 && i<65+BlockSizes.values().length)
		{
			level = i-65; 
		} 
		else
			return null;
		
		return  BlockSizes.values()[level + start_physical_level]; 
	}
	
	public static int getLevel(BlockSizes _b_s)
	{
		return _b_s.getLevel();
	}
	
	public void loadCriticalValues(String critical_conf_file)
	{
		File pcritical_file = new File(critical_conf_file);     
		FileInputStream   pInStream=null;

		try { 
			pInStream = new FileInputStream(pcritical_file );
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}

		Properties p = new Properties();

		try {
			p.load(pInStream );  
		} catch (IOException e) 
		{
			e.printStackTrace(); 
		}
		/*
		 * enumerating all properties
		 */
		Enumeration enu = p.propertyNames();  
		 
		System.out.println("[INFO]: Loading LunarDB system core params......"); 
		while( enu.hasMoreElements())
		{
			//String key = StrLegitimate.purifyStringEn((String)enu.nextElement()).trim();
			//String value =StrLegitimate.purifyStringEn( p.getProperty(key) ).trim();
			String key = ((String)enu.nextElement()).trim();
			String value = p.getProperty(key).trim();
			
			switch(key)
			{
				case "mani_file_bit_len": 
					int bit_length = Math.min( Math.abs( Integer.valueOf(value)), 31);
					max_4k_blocks_in_seg = (Math.abs( 1 << bit_length)) >> 12;
					seg_file_size = Math.abs( 1 << bit_length);
					break;
				case "block_ultimate_level":
					int level_count = Math.min( BlockSizes.values().length - DBFSProperties.start_physical_level, Math.abs(Integer.valueOf(value)));
					max_level_count =  Math.max(2, level_count);
					break;
				case "records_table_file_bit_len": 
					records_table_file_size = Math.abs( ((long)1) << Math.min(Long.valueOf(value), 63)); 
					break;
			}
			
			//System.out.println(key + "=" + value ); 
		}
		System.out.println("[SUCEED]: LunarDB system core params loaded."); 
		
		/*
		 * check the max_level and the segment file size in byte.
		 * Since the three variables (mani_file_bit_len, 
		 * records_table_file_bit_len, block_ultimate_level) 
		 * are not allowed to change after creation,
		 * this code to reset seg_file_size is ok. 
		 * 
		 * Every time opens the db, seg_file_size will be reset to 
		 * getBlockSize(max_level).getSize() * 16;
		 * 
		 */
		if(getBlockSize( start_physical_level + max_level_count-1).getSize() > seg_file_size)
			seg_file_size = getBlockSize(start_physical_level + max_level_count-1).getSize() * 16;
		
		try {
			pInStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void loadConfig(String conf_file)
	{
		File p_conf_file = new File(conf_file);     
		FileInputStream   pInStream=null;

		try { 
			pInStream = new FileInputStream(p_conf_file );
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}

		Properties p = new Properties();

		try {
			p.load(pInStream );  
		} catch (IOException e) 
		{
			e.printStackTrace(); 
		}
		/*
		 * enumerating all properties
		 */
		Enumeration enu = p.propertyNames();  
		 
		System.out.println("[INFO]: Loading LunarDB runtime params......"); 
		while( enu.hasMoreElements())
		{
			//String key = StrLegitimate.purifyStringEn((String)enu.nextElement()).trim();
			//String value = StrLegitimate.purifyStringEn(p.getProperty(key)).trim();
			String key = ((String)enu.nextElement()).trim();
			String value = p.getProperty(key).trim();
			
			switch(key)
			{
				/*
				case "root_path": 
					db_root_path = value.trim(); 
					if(!db_root_path.endsWith("/"))
						db_root_path = db_root_path + "/";
					break;
					*/
				case "database_name":
					db_name = value.trim(); 
					break;
				case "port": 
					port = Math.abs( Integer.valueOf(value));
					break;
				case "linux_X86_native_enabled":
					if(!value.equalsIgnoreCase("no"))
						native_mode = true;
					else
						native_mode = false;
					break;
				case "multi_process_support_enabled":
					break;
				case "bit_buff_length":
					bit_buff_len = Math.min( Math.abs(Integer.valueOf(value)) , 31);
					break;
				case "cache_blocks_in_memory":
					max_4k_blocks_in_cache = Math.abs(1 << Math.min(Math.abs(Integer.valueOf(value)), 31));
					mem_cache_size = ((long)max_4k_blocks_in_cache) * BlockSizes.block_4k.getSize(); 
					break;
				case "cache_keys_in_memory":
					max_cache_keys_in_memory = Math.abs(1 << Math.min(Math.abs(Integer.valueOf(value)), 31));
					break;
				case "cache_records_in_memory":
					max_records_in_memory = Math.abs(1 << Math.min(Math.abs(Integer.valueOf(value)), 31));
					break;
				case "internal_cache_concurrent_level":
					concurrent_level = Math.abs(Integer.valueOf(value));
					break;
				case "ultimate_socket_concurrent_connections":
					break;
				case "search_engine_mode":
					if(!value.equalsIgnoreCase("off"))
						search_engine_mode = true ;
					else
						search_engine_mode = false; 
					break;
				case "rt_mode":
					if(!value.equalsIgnoreCase("off"))
						real_time_mode = true ;
					else
						real_time_mode = false; 
					break;
				
				case "rt_mem_page_size":	
					int page_size = 1<< (Math.max(Math.abs(Integer.valueOf(value)), 12)) ;
					//TODO
					rt_index_order = 320;
					break;
				case "rt_virtual_mem_enabled":
					if(!value.equalsIgnoreCase("off"))
						rt_virtual_mem_enabled = true ;
					else
						rt_virtual_mem_enabled = false;
					break;
				case "rt_max_memory": 
					rt_max_memory = 1<< (Math.max(Math.abs(Integer.valueOf(value)), 12)) ;
					break;
				case "rt_max_virtual_pte": 
					rt_max_virtual_pte = 1<< (Math.max(Math.abs(Integer.valueOf(value)), 12)) ;
					break; 
				case "rt_vm_swap": 
					rt_vm_swap = value.trim();   
					break;  
				case "rt_precision":
					rt_precision = Math.max(Math.abs(Integer.valueOf(value)), 3) ;
					break;
				case "rt_threads":
					rt_threads =  Math.max(Math.abs(Integer.valueOf(value)), 1) ;
					break;
			}
			
			//System.out.println(key + "=" + value ); 
			
		}
		System.out.println("[SUCEED]: LunarDB runtime params loaded."); 
		
		try {
			pInStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
	
	public void createConfig(String conf_creation)
	{
		File p_conf_file = new File(conf_creation);     
		FileInputStream   pInStream=null;

		try { 
			pInStream = new FileInputStream(p_conf_file );
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}

		Properties p = new Properties();

		try {
			p.load(pInStream );  
		} catch (IOException e) 
		{
			e.printStackTrace(); 
		}
		 
		/*String root_path = StrLegitimate.purifyStringEn( p.getProperty("root_path")).trim(); 
		if(!root_path.endsWith("/"))
			root_path = root_path+"/";
		
		db_root_path
		String conf_file = root_path + p.getProperty("database_name").trim() + "/" + runtime_conf;
		*/ 
		String conf_file = db_root_path + p.getProperty("database_name").trim() + "/" + runtime_conf;
		
		FileOutputStream fo = null; 
        FileChannel channel_in = null; 
        FileChannel channel_out = null;

        try { 
            fo = new FileOutputStream(conf_file); 
            channel_out = fo.getChannel();  
            channel_in = pInStream.getChannel(); 

            channel_in.transferTo(0, channel_in.size(), channel_out); 

        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            try { 
            	pInStream.close(); 
                channel_in.close(); 
                fo.close(); 
                channel_out.close();

            } catch (IOException e) {

                e.printStackTrace();

            }

        }
	}
	 
}

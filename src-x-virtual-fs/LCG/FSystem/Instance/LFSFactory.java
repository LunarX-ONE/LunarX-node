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
 

package LCG.FSystem.Instance;

import java.io.IOException;

import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.FSystem.Manifold.LFSManifold;

/*
 * 
# ------------ file system level ------------------#
# Maximum block level controlls the size of blocks #
# of Lunar virtual file system.			           #
# -level 0: block size = 64 byte,                  #
# -level 1: block size = 512 byte,                 #
# -level 2: block size = 4K byte,                  #
# -level 3: block size = 32K byte,                 #
# -level 4: block size = 256K byte,                #
# -level 5: block size = 2048K byte,               #
# -level 6: block size = 16384K byte,              #
# (level 6 is NOT supported yet for                #
# the memory concerns)                             #
#                                                  #
# Maximum level 2 serves well for most of the 	   #
# applications with lunardb as their storage  	   #
# and search engine                                #
# ------------------------------------------------ #
 * 
 */

public class LFSFactory {

	private final String root_path;
	private final String table_name;
	private final String mode; 
	private final int bufbitlen; 
	private final boolean native_mode; 
	private final DBFSProperties dbfs_prop_instance;
	
	public LFSFactory(String _root_path, 
			String _table_name, 
			String _mode, 
			int _bufbitlen, 
			boolean _native_mode, 
			DBFSProperties _dbfs_prop_instance)
	{
		root_path = _root_path;
		table_name = _table_name;
		mode = _mode;
		bufbitlen = _bufbitlen;
		native_mode = _native_mode;
		dbfs_prop_instance = _dbfs_prop_instance;
	}
	
	public LFSManifold generateManifoldPatch(int level) throws IOException
	{
		int bit_len = 12;
		switch(level+ dbfs_prop_instance.start_physical_level) 
		{
	 
		case 0:
			bit_len = bufbitlen<=12 ? 12:bufbitlen;
			return new LFS64B(root_path, table_name, mode, bit_len, native_mode, dbfs_prop_instance);
		case 1:
			bit_len = bufbitlen<=12 ? 12:bufbitlen;
			return new LFS512B( root_path, table_name, mode, bit_len, native_mode, dbfs_prop_instance);
		case 2:
			bit_len = bufbitlen<=12 ? 12:bufbitlen;
			return new LFS4K( root_path, table_name, mode, bit_len, native_mode, dbfs_prop_instance);
		case 3:
			bit_len = bufbitlen<=15 ? 16:bufbitlen; 
			return new LFS32K( root_path, table_name, mode, bit_len, native_mode, dbfs_prop_instance);
		case 4:
			bit_len = bufbitlen<=18 ? 19:bufbitlen; 
			return new LFS256K( root_path, table_name, mode, bit_len, native_mode, dbfs_prop_instance);
		case 5:
			bit_len = bufbitlen<=21 ? 22:bufbitlen;  
			return new LFS2M( root_path, table_name, mode, bit_len, native_mode, dbfs_prop_instance);
		 
		/*
		case 0:
			return new LFS512B( root_path, table_name, mode, bufbitlen, native_mode, dbfs_prop_instance);
		case 1: 
			return new LFS32K( root_path, table_name, mode, bufbitlen, native_mode, dbfs_prop_instance);
		case 2:
			return new LFS2M( root_path, table_name, mode, bufbitlen, native_mode, dbfs_prop_instance);
			
		*/
		//case 6:
		//	return new LFS16M( root_path, table_name, mode, bufbitlen, native_mode);
			
		}
		return null;
	} 
	
}

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

package LCG.FSystem.Instance;

import java.io.IOException;

import LCG.Concurrent.CacheBig.Controller.Instance.Block2MCache;
import LCG.Concurrent.CacheBig.Controller.Instance.Block4KCache;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.FSystem.Manifold.LFSManifold;

public class LFS2M extends LFSManifold{ 
	static String _suggested_middle_name = ".2M";
	public LFS2M(String _root_path, 
			String _table_name,
			String _mode, 
			int bufbitlen, 
			boolean native_mode,
			DBFSProperties _dbfs_prop_instance) throws IOException {
		super(_root_path, _table_name+_suggested_middle_name, 
				_mode, bufbitlen, 
				BlockSizes.block_2M, Block2MCache.getInstance(_dbfs_prop_instance),
				native_mode,
				_dbfs_prop_instance); 
	} 
} 

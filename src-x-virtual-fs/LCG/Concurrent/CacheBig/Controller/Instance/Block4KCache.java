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

package LCG.Concurrent.CacheBig.Controller.Instance;

import LCG.Concurrent.CacheBig.Controller.impl.BlockCache;
import LCG.Concurrent.CacheBig.Controller.impl.BlockCacheLazyAllocate;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;

public class Block4KCache extends BlockCacheLazyAllocate{

 
	/*
	 * will not use the singleton pattern, 
	 * since each manifold has a block cache, 
	 * and there are many manifolds like with 4k blocks, 
	 * 64b blocks, etc
	 */ 
	
 
	public static Block4KCache getInstance(DBFSProperties dbfs_prop_instance) 
	{
		return new Block4KCache(dbfs_prop_instance); 
	} 
	
    protected Block4KCache(DBFSProperties dbfs_prop_instance)
    {
    	super(DBFSProperties.BlockSizes.block_4k, 
    			dbfs_prop_instance.max_4k_blocks_in_cache,
    			dbfs_prop_instance);
    }
 
}
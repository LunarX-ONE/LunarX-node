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

package LCG.Concurrent.CacheBig.Controller.Instance;

import LCG.Concurrent.CacheBig.Controller.impl.BlockCache;
import LCG.Concurrent.CacheBig.Controller.impl.BlockCacheLazyAllocate;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;

public class Block512BCache extends BlockCacheLazyAllocate{  
	 
	public static Block512BCache getInstance(DBFSProperties dbfs_prop_instance) 
	{
		return new Block512BCache(dbfs_prop_instance); 
	} 
	
    protected Block512BCache(DBFSProperties dbfs_prop_instance)
    {
    	super(DBFSProperties.BlockSizes.block_512b, 
    			dbfs_prop_instance.mem_cache_size >> BlockSizes.block_512b.getBitLen(),
    			dbfs_prop_instance);
    }

}
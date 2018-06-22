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

public class Block16MCache extends BlockCacheLazyAllocate{  
	 

	public static Block16MCache getInstance(DBFSProperties dbfs_prop_instance) 
	{
		//return new Block16MCache( dbfs_prop_instance); 
		return null;
	} 
 
    protected Block16MCache(DBFSProperties dbfs_prop_instance)
    {  
    	
    	super( BlockSizes.block_16M,
    			Math.max( dbfs_prop_instance.mem_cache_size / BlockSizes.block_16M.getSize() ,
    					//at least 4 blocks in memory for this very big block 
						4),  
    			dbfs_prop_instance);
    	
    } 

}
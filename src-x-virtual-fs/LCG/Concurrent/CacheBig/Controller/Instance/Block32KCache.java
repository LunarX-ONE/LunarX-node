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

public class Block32KCache extends BlockCacheLazyAllocate{  
	
	 
	public static Block32KCache getInstance(DBFSProperties dbfs_prop_instance ) 
	{
		return new Block32KCache(dbfs_prop_instance); 
	} 
	
    protected Block32KCache(DBFSProperties dbfs_prop_instance )
    {     			 
    	super( BlockSizes.block_32k,
    			Math.max( dbfs_prop_instance.mem_cache_size / BlockSizes.block_32k.getSize() ,
						8),/* at least 8 blocks in memory*/
    			dbfs_prop_instance);
    }

}
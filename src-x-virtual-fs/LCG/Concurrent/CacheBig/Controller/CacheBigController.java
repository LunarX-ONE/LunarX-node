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


package LCG.Concurrent.CacheBig.Controller;

import LCG.Concurrent.CacheBig.Controller.Instance.Block256KCache;
import LCG.Concurrent.CacheBig.Controller.Instance.Block2MCache;
import LCG.Concurrent.CacheBig.Controller.Instance.Block32KCache;
import LCG.Concurrent.CacheBig.Controller.Instance.Block4KCache;
import LCG.Concurrent.CacheBig.Controller.Instance.Block512BCache;
import LCG.Concurrent.CacheBig.Controller.Instance.Block64BCache;
import LCG.Concurrent.CacheBig.Controller.impl.BlockCache;
import LCG.Concurrent.CacheBig.Controller.impl.BlockCacheLazyAllocate;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;

public class CacheBigController {
	
	public static BlockCacheLazyAllocate getLevelCache(BlockSizes _b_s,
						DBFSProperties dbfs_prop_instance)
	{ 
		//BlockCache _cache; 
		BlockCacheLazyAllocate _cache;
		switch(_b_s)
		{
			//case block_64b:
			//	_cache = Block64BCache.getInstance(dbfs_prop_instance);
			//	break;
			case block_512b:
				_cache = Block512BCache.getInstance(dbfs_prop_instance);
				break;
			//case block_4k:
			//	_cache = Block4KCache.getInstance(dbfs_prop_instance);
			//	break;
			case block_32k:
				_cache = Block32KCache.getInstance(dbfs_prop_instance);
				break;
			//case block_256k:
			//	_cache = Block256KCache.getInstance(dbfs_prop_instance);
			//	break;
			case block_2M:
				_cache = Block2MCache.getInstance(dbfs_prop_instance);
				break;
			default:
				_cache = null;
				break;
		}
		
		return _cache;
	}
	
	 

}

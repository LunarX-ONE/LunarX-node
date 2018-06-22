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


package LCG.FSystem.UnSure;

import java.io.IOException;
import java.util.Vector;

import LCG.Concurrent.CacheBig.Controller.impl.BlockCache;
import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.FSystem.AtomicStructure.BlockGeneric;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.FSystem.Manifold.LFSBaseLayer;
import LCG.FSystem.Manifold.TManifold;
import LCG.StorageEngin.IO.L1.IOStream; 
import LCG.StorageEngin.Serializable.IObject;
import LCG.StorageEngin.Serializable.Impl.Int4Byte;
 

public class BlockVariant extends BlockGeneric{  
	
	public BlockVariant(int _size, DBFSProperties dbfs_prop_instance) throws IOException
	{  
		super(new EntryV(_size), dbfs_prop_instance);  
	} 
	
	public BlockVariant(EntryV _entry, DBFSProperties dbfs_prop_instance) throws IOException
	{  
		super(_entry, dbfs_prop_instance);  
	} 
	
	//for variant size block only. Do not apply to the fixed size block 
	public int expandSize(int _size)
	{
		return s_v_entry.expandBlockSize( _size); 
	}
}

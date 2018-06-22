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


package LCG.FSystem.AtomicStructure;

import java.io.IOException;
import java.util.Vector;

import LCG.Concurrent.CacheBig.Controller.impl.BlockCache;
import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.FSystem.Def.DBFSProperties;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.FSystem.Manifold.LFSBaseLayer;
import LCG.FSystem.Manifold.TManifold;
import LCG.StorageEngin.IO.L1.IOStream; 
import LCG.StorageEngin.Serializable.IObject;
import LCG.StorageEngin.Serializable.Impl.Int4Byte;
 

public class BlockSimple extends BlockGeneric{ 
	 
	public BlockSimple(BlockSizes b_s, int index_in_pool, DBFSProperties dbfs_prop_instance)  
	{ 
		super(new EntrySimple(b_s), index_in_pool, dbfs_prop_instance);	 
	} 
	  
	public long BlockBeginOffset()
	{
		int size = s_v_entry.getBlockSize();
		return (long)  size * (s_obj_id % (dbfs_prop_instance.seg_file_size/size));
	}
	
	public EntrySimple getEntry()
	{
		return (EntrySimple)super.getEntry();
	}
	  
	public long getNext()
	{
		return ((EntrySimple)this.s_v_entry).s_next_id;
	}
	
	public void setNext(long _next_id)
	{ 
		((EntrySimple)this.s_v_entry).s_next_id = _next_id;
		((EntrySimple)this.s_v_entry).is_dirt = true;
	}
	
	public long getPrev()
	{
		return ((EntrySimple)this.s_v_entry).s_previous_id;
	}
	
	public void setPrev(long _prev_id)
	{
		((EntrySimple)this.s_v_entry).s_previous_id = _prev_id;
		((EntrySimple)this.s_v_entry).is_dirt = true;
	}
	
	public int getNextLevel()
	{
		return ((EntrySimple)this.s_v_entry).s_next_level; 
	}
	
	public void setNextLevel(int _next_level)
	{
		((EntrySimple)this.s_v_entry).s_next_level = _next_level; 
		((EntrySimple)this.s_v_entry).is_dirt = true;
	}
	
	public int getPrevLevel()
	{
		return ((EntrySimple)this.s_v_entry).s_prev_level; 
	}
	
	public void setPrevLevel(int _prev_level)
	{
		((EntrySimple)this.s_v_entry).s_prev_level = _prev_level; 
		((EntrySimple)this.s_v_entry).is_dirt = true;
	}
	
}

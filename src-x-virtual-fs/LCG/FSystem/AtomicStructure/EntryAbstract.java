package LCG.FSystem.AtomicStructure;

import java.io.IOException;

import LCG.FSystem.Def.DBFSProperties.BlockSizes;
import LCG.StorageEngin.IO.L1.IOStream; 
import LCG.StorageEngin.Serializable.ILongObject;
import LCG.StorageEngin.Serializable.IObject;

public abstract class EntryAbstract extends ILongObject { 
	public Byte s_status; //can be available = 'A' or unavailable = 'U', plus block size info 
	public Byte available; 
	
	public int s_rec_count;//how many records in this block
	public int s_saved_offset = 0; //how many bytes used(saved) in this block, except the entry itself 
	protected boolean is_dirt = false;
	protected byte[] byte_buff; 
	
	abstract public void returnToPool();
	abstract public void setAvailable();
	abstract public void setUnavailable();
	abstract public int getBlockSize();
	abstract public int getBlockLevel();
	abstract public int expandBlockSize(int _size); 
	abstract public void setID(long _id);	 
	abstract public boolean isDirt();
	abstract public void setClean();
	abstract public long getMyPosition();
	abstract public void setMyPosition(long _pos);
	abstract public Byte getAvailability();
}

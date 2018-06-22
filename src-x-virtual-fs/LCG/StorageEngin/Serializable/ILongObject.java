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


package LCG.StorageEngin.Serializable;

import java.io.IOException;

import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.IO.L1.IOStream;
import LCG.StorageEngin.IO.L1.IOStreamNative; 

public abstract class ILongObject<I extends IOInterface> { 
	 
	protected long s_obj_id;
	public ILongObject() {}
	
	public int sizeInBytes()
	{
		return 8;
	}
	/*
	abstract public void Read(IOStream io_v) throws IOException;
    abstract public void Write(IOStream io_v) throws IOException;
    abstract public void Read(IOStreamNative io_native) throws IOException;
    abstract public void Write(IOStreamNative io_native) throws IOException;
   
    abstract public void Read(MemStream io_m) throws IOException;
    abstract public void Write(MemStream io_m) throws IOException;
    */
	abstract public void Read(I io_v) throws IOException;
    abstract public void Write(I io_v) throws IOException;
    
    public long getID()
    {
    	return this.s_obj_id;
    }
    
    abstract public void setID(long _id);
    
 
}

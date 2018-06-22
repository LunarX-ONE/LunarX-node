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

import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.FSystem.Manifold.LFSBaseLayer;
import LCG.FSystem.Manifold.TManifold;
import LCG.StorageEngin.IO.L1.IOStream; 
import LCG.StorageEngin.Serializable.ILongObject;
import LCG.StorageEngin.Serializable.IObject;

public abstract class IFSObject extends ILongObject{  
	final public MemoryStrongReference g_msr;
	protected boolean read_only = false;
	protected boolean is_dirt = false;
	
	public IFSObject()
	{
		g_msr = null;
	}
	public IFSObject(int index_in_pool) 
	{
		g_msr = MemoryStrongReference.getReference(index_in_pool);
	}
	
	public MemoryStrongReference getMemReference()
	{
		return this.g_msr;
	} 

	public void setReadOnly()
	{
		this.read_only = true;
	}
	
	public void setReadWrite()
	{
		this.read_only = false;
	}
	
	public boolean isReadOnly()
	{
		return this.read_only;
	}
	
	public boolean isDirt()
	{
		return this.is_dirt;
	}
	 
	abstract public void read(LFSBaseLayer al_file) throws IOException;
	abstract public void write(LFSBaseLayer al_file) throws IOException;
	
}

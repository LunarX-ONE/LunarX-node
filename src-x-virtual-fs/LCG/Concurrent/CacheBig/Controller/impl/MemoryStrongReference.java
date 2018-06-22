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


package LCG.Concurrent.CacheBig.Controller.impl;
 
import java.util.concurrent.atomic.AtomicLong;

import LCG.Concurrent.CacheBig.Controller.MemoryReference;

public class MemoryStrongReference extends MemoryReference {
	 
	public static MemoryStrongReference getReference(int index )
	{
		return new MemoryStrongReference(index);
	}
	
	private MemoryStrongReference(int index )
	{
		super(index);
		this._free = true; 
		this._reference_count = new AtomicLong(0);
	}
	 
	public synchronized boolean free()
	{ 
		/*
		 * still has reference, can not free
		 */
		if(this._reference_count.get()>0)
			return false;
		else
		{
			this._free = true; 
			return this._free;
		} 
	}
	
	public synchronized void forceFree()
	{
		this._free = true; 
		this._reference_count.set(0); 
	}
	
	public synchronized void occupy()
	{
		this._free = false; 
	}
	
	public synchronized int getIndexInPool()
	{
		return this._index_in_pool;
	}
	
	public synchronized boolean getStatus()
	{
		return this._free;
	}

	@Override
	public synchronized void increaseMemoryRef() {
		this._reference_count.getAndIncrement(); 
	}

	@Override
	public synchronized void decreaseMemoryRef() {
		this._reference_count.getAndDecrement(); 
	}

}

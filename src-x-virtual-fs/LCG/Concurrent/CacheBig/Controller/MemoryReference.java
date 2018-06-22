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

import java.util.concurrent.atomic.AtomicLong;


public abstract class MemoryReference {
	protected boolean _free;
	protected AtomicLong _reference_count;
	protected final int _index_in_pool; 
	 
	protected MemoryReference(int index)
	{
		this._index_in_pool = index;
	}
	abstract public boolean free(); 
	
	abstract public void occupy();
	
	abstract public void increaseMemoryRef();
	abstract public void decreaseMemoryRef();
	
	abstract public int getIndexInPool();

}

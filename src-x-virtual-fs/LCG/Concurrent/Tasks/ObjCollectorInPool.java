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


package LCG.Concurrent.Tasks; 

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import LCG.Concurrent.CacheBig.Controller.Instance.Block4KCache;
import LCG.Concurrent.CacheBig.Controller.impl.BlockCache;
import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.Manifold.TManifold;


public class ObjCollectorInPool implements Callable<Void> {  

	private BlockCache g_cache;
	private TManifold g_outer_storage;  
	 
	//private MemoryStrongReference msr_waiting_for_free;
	private BlockingQueue<BlockSimple> msr_waiting_for_free; 
	
	public ObjCollectorInPool(BlockingQueue<BlockSimple> _msr_waiting, TManifold outer_storage, BlockCache _cache) {  
		this.g_cache = _cache; 
		this.g_outer_storage = outer_storage;
		 
		this.msr_waiting_for_free = _msr_waiting; 
	}  
	 
	@Override
	public Void call()   
	{ 
		//for (;;) 
		//{
			while(!this.msr_waiting_for_free.isEmpty())
			{
				try
				{
					BlockSimple removed = this.msr_waiting_for_free.take();
					long id_to_remove = removed.getID();
					if(!removed.isDirt())
					{ 
						this.g_cache.tryReleaseMemory(removed.getMemReference(), removed.getID());
						System.out.println("Thread " + Thread.currentThread().getName() + 
								" released a removed block " 
								+ id_to_remove + ".");
						
						continue;
					} 
					
					long startttt = System.nanoTime(); 
					
					this.g_outer_storage.writeObj(removed, removed.getID());
					
					System.out.println("finished writing a dirt block that Spends: "
			                + (System.nanoTime()-startttt) 
			                + "(ns)");
					this.g_cache.tryReleaseMemory(removed.getMemReference(), removed.getID()); 
					System.out.println("Thread " + Thread.currentThread().getName() + 
							" released a removed dirt block " 
							+ id_to_remove + ".");
					continue;
				}
				catch(InterruptedException ie)
				{
					Thread.currentThread().interrupt();
				} 
				catch (IOException e) {
					System.out.println("Error: failed flushing dirt block to outer storage.");
					e.printStackTrace();
				}  
			} 
			return null; 
        //}		
	}  
}
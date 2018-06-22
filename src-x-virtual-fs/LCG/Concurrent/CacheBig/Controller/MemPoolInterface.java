package LCG.Concurrent.CacheBig.Controller;
 
import java.io.IOException;

import LCG.Concurrent.CacheBig.Controller.impl.MemoryStrongReference;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.AtomicStructure.IFSObject;
import LCG.FSystem.Manifold.TManifold;
import LCG.StorageEngin.Serializable.IObject;

public abstract class MemPoolInterface<V extends IObject> {

	//protected final V[] g_obj_pool;
	
	//protected MemPoolInterface(int _capacity)
	//{
	//	this.g_obj_pool =(V[]) new IObject[_capacity]; 
	//}
	
	//abstract public MemoryReference acquirFreeReference() throws InterruptedException; 
    
	abstract public IFSObject acquirFreeObjMemory() throws InterruptedException; 
	
	abstract public boolean tryReleaseMemory(MemoryStrongReference mr_to_obj, long obj_id) throws InterruptedException;
    
	
	//abstract public IFSObject getMemoryForBlock(MemoryStrongReference m_r);
	
	abstract public IFSObject getObj(long obj_id, TManifold outer_storage) throws IOException;
	
	abstract public void returnObj(MemoryStrongReference m_r, TManifold outer_storage) throws IOException;
	
	abstract public int length();
    
	//abstract public long size(); 
	
	abstract public void shutDown();
}

package LunarX.Memory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import LCG.FSystem.Def.DBFSProperties;

public class Closable {
	 
	protected AtomicBoolean shutdown_called;
	protected AtomicLong  critical_reference;

	protected void init() { 
		this.shutdown_called = new AtomicBoolean(false);
		this.critical_reference = new AtomicLong(0);
	} 
	
	public void increaseCriticalReference()
	{
		critical_reference.getAndIncrement();
	}
	
	public void decreaseCriticalReference()
	{
		critical_reference.getAndDecrement();
	}
}

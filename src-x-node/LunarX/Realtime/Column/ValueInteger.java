package LunarX.Realtime.Column;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import LCG.StorageEngin.IO.L0.IOInterface;

public class ValueInteger {
	
	/*
	 * belongs to which record
	 */
	private int rec_id; 
	
	private int data;
	
	private AtomicInteger s_version = new AtomicInteger();
	
	//private byte record_status;
	
	public ValueInteger( ) {
		data = -1;
	 
		this.rec_id = -1;
	}

	public ValueInteger(int id, int data, int new_version) {
		this.rec_id = id;
		this.data = data; 
		s_version.set(new_version);
	}

	public int getRecID()
	{
		return this.rec_id;
	}

	public AtomicInteger getVersion()
	{
		return this.s_version ;
	}
	 
	
	public void Read(IOInterface io_v) throws IOException {
		synchronized(this)
		{
			synchronized(io_v)
			{ 
				this.rec_id = io_v.read4ByteInt(); 
				this.s_version.set(io_v.read4ByteInt());
				
				data = io_v.read4ByteInt();
				 
			}
		}
	}
	
	
 
	public void Write(IOInterface io_v) throws IOException {
		synchronized(this)
		{ 
			synchronized(io_v)
			{ 
				io_v.write4ByteInt(this.rec_id);
				io_v.write4ByteInt(this.s_version.get());
				
				io_v.write4ByteInt(this.data); 
			} 
		}
	} 
	
	public int recLength()
	{ 
		return 12;
		/*
		 * for testing multiple segment files, we let one record big
		 */
		//return 800;
	}
	
	public int recData()
	{
		return this.data;
	} 
	 
}

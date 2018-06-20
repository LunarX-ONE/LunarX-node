package LunarX.RecordTable.Structure;

import java.io.IOException;

import LCG.StorageEngin.IO.L0.IOInterface;

public abstract class RecordObject{
	protected int rec_id;
	//protected int transaction_id;
	protected int s_trx_id;
	protected long s_trx_roll_addr;
	
 

	abstract public void Read(IOInterface io_v) throws IOException ;   

	abstract public void Write(IOInterface io_v) throws IOException ;
 

	 
}

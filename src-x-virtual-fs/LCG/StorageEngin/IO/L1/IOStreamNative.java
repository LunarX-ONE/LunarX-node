package LCG.StorageEngin.IO.L1;

import java.io.IOException; 
 
import LCG.StorageEngin.IO.L0.IOChannelMBBNative64;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;

public class IOStreamNative extends IOChannelMBBNative64 { 
	 
	private VariableGeneric local_v_g;
	public IOStreamNative(String name, String mode, int bufbitlen) throws IOException {
		super(name, mode, bufbitlen);
		local_v_g = new VariableGeneric(this);
	}
	
	public void write1ByteInt(int i) throws IOException { 
    	super.write1ByteInt(this._physical_mem_addr, i);
    }
	
	public int read1ByteInt() throws IOException { 
        return super.read1ByteInt(_physical_mem_addr);
    }  
	 
    public final void write4ByteInt(int i) throws IOException {  
    	super.write4ByteInt(_physical_mem_addr, i);
    } 
   
    public final int read4ByteInt() throws IOException {  
        return super.read4ByteInt(_physical_mem_addr);
    }
    
    public final void TransformIntTo4Byte(byte[] b, int from, int i)
    { 
    	local_v_g.TransformIntTo4Byte(b, from, i);
    }
    
    public final int Transform4ByteToInt(byte[] b, int from)
    {
    	return local_v_g.Transform4ByteToInt(b, from);
    }
    
    public final long Transform8ByteToLong(byte[] b, int from)
    {
    	return local_v_g.Transform8ByteToLong(b, from);
    } 
    
    public void writeChars(String s, int start, int length) throws IOException {
    	local_v_g.writeChars(s, start, length);
    }
    
    public void writeChars(char[] s, int start, int length) throws IOException {
    	local_v_g.writeChars(s, start, length);
    }
    
    public void readChars(char[] buffer, int start, int length)
    		throws IOException {
    	local_v_g.readChars(buffer, start, length);
    	
    }
    
    public void WriteLong(long i) throws IOException {
    	//local_v_g.WriteLong(i);
    	//write4ByteInt((int) (i >> 32));
        //write4ByteInt((int) i);
    	WriteLong(_physical_mem_addr, i);
    }
    
    public long ReadLong() throws IOException {
    	//return local_v_g.ReadLong(); 
        return ReadLong(_physical_mem_addr);
         
    }  

}

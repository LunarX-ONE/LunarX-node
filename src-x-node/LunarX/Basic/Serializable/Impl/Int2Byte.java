package LunarX.Basic.Serializable.Impl;

import java.io.IOException;

import LCG.MemoryNative64.ByteArray;
import LCG.StorageEngin.IO.L0.IOInterface;

public class Int2Byte {
	short value;
	byte[] value_bytes;
	
	public Int2Byte()
	{
		value = -1;
		value_bytes = new byte[2];
	}
	
	public Int2Byte(short i)
	{ 
		value = i; 
		value_bytes = new byte[2];
		value_bytes[1] = (byte) (i >> 8);  
		value_bytes[0] = (byte) (i >> 0);  
	}
	
	public void Read(IOInterface io_v) throws IOException
	{
		if(io_v.read(value_bytes) != -1)
			value = (short) (((value_bytes[1] << 8) | value_bytes[0] & 0xff)); 
		else
			value = -1;
	} 
	 
	public void Read(byte[] b, int from)
	{
		if((from+2)>=b.length || b==null)
			value = -1;
    	else
    	{
    		value_bytes[0] = b[from];
    		value_bytes[1] = b[from + 1] ;
    		
    		value = (short) (((value_bytes[1] << 8) | value_bytes[0] & 0xff)); 
    	}
	} 
	
	public void Read(ByteArray byte_array, int from) throws Exception
	{ 
		//value_bytes[0] = b[from];
		//value_bytes[1] = b[from + 1] ;
		value_bytes[0] = byte_array.readByte(from);
		value_bytes[1] = byte_array.readByte(from+1);
    		
		value = (short) (((value_bytes[1] << 8) | value_bytes[0] & 0xff)); 
    	 
	} 
	 
	public void Write(IOInterface io_v) throws IOException { 
		io_v.write(value_bytes); 
	} 
	
	public void Write(byte[] b, int from)
	{ 
		b[from] = value_bytes[0];
		b[from + 1] = value_bytes[1]; 
	} 
	
	public void Write(ByteArray byte_array, int from) throws Exception
	{  
		byte_array.putByte(from, value_bytes[0]);
		byte_array.putByte(from+1, value_bytes[1]); 
	} 
	
	public short Get()
	{
		return value ;  
	} 
	
	public int Size()
	{
		return 2;
	}
 
}

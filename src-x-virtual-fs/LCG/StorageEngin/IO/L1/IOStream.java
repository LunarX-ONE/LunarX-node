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



package LCG.StorageEngin.IO.L1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import LCG.StorageEngin.IO.L0.IOChannelMBB; 
import LCG.StorageEngin.Serializable.IObject;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
 

public class IOStream extends IOChannelMBB {  
	
	private VariableGeneric local_v_g;
	
	public IOStream(File file) throws IOException, FileNotFoundException {
		super(file);
		// TODO Auto-generated constructor stub
	}

	public IOStream(String name, String mode, int bufbitlen) throws IOException  {
		super(name, mode, bufbitlen);
		local_v_g = new VariableGeneric(this);
    } 
	
	 
	public void write1ByteInt(int i) throws IOException { 
    	local_v_g.write1ByteInt(i); 
    }
	
	public int read1ByteInt() throws IOException {
		 
        return local_v_g.read1ByteInt();
    } 
	
	 
    
     
    
	 
    public final void write4ByteInt(int i) throws IOException {  
    	//local_v_g.write4ByteInt(i);
    	 
    	int len = 4;
    	long write_end_pos = this.current_pos + len - 1;

        if (write_end_pos <= this.buff_end_pos) { 
        	local_v_g.TransformIntTo4Byte(this.buff, (int)(this.current_pos - this.buff_start_pos), i);
        	//this.mbb.position((int)(this.current_pos - this.buff_start_pos));
        	//this.mbb.putInt(i);
            this.if_buff_dirty = true;
            if(this.buff_used_size < (int)(write_end_pos - this.buff_start_pos + 1) )
            	this.buff_used_size = (int)(write_end_pos - this.buff_start_pos + 1);
  
        } else { // b[] not in cur buf 
        	mbb = file_channel.map(FileChannel.MapMode.READ_WRITE, this.current_pos, len);  
    		mbb.putInt(i); 
    		UnMap();
        }

        if (write_end_pos > this.file_end_pos)
            this.file_end_pos = write_end_pos;

        this.seek(write_end_pos+1); 
    }
    
   
    public final int read4ByteInt() throws IOException {  
        //return local_v_g.read4ByteInt(); 
        
        int len = 4;
        int val = -1;
        long readendpos = this.current_pos + len - 1;

        if (readendpos <= this.buff_end_pos && readendpos <= this.file_end_pos ) 
        {  
        	val = Transform4ByteToInt(this.buff, (int)(this.current_pos - this.buff_start_pos));
        	//this.mbb.position((int)(this.current_pos - this.buff_start_pos));
        	//val = this.mbb.getInt();
        } else {  
            if (readendpos > this.file_end_pos) { // read b[] part in file
            	len = (int)(this.file_end_pos - this.current_pos + 1); 
            	if(len <=0)
            		return -1;//end of the file, nothing can be read
            } 
            
            this.mbb = file_channel.map(FileChannel.MapMode.READ_ONLY, this.current_pos, len);  
            val = mbb.getInt(); 
    		UnMap();
           
            readendpos = this.current_pos + len - 1;
        }
        this.seek(readendpos + 1);
        return val;
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

    
    public final void writeVInt(int i) throws IOException { 
        local_v_g.writeVInt(i);
    } 
    
    public final int readVInt() throws IOException {
        return local_v_g.readVInt();
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
    
    public final void WriteLong(long i) throws IOException {
    	//local_v_g.WriteLong(i);
    	write4ByteInt((int) (i >> 32));
        write4ByteInt((int) i);
    }
    
    public final long ReadLong() throws IOException {
    	//return local_v_g.ReadLong(); 
        
        int len = 8;
        long val = -1;
        long readendpos = this.current_pos + len - 1;

        if (readendpos <= this.buff_end_pos && readendpos <= this.file_end_pos ) 
        { 
        	val = Transform8ByteToLong(this.buff, (int)(this.current_pos - this.buff_start_pos));
        	//this.mbb.position((int)(this.current_pos - this.buff_start_pos));
        	//val = this.mbb.getLong();
        } else { 
        	// read b[] size > buf[]
            if (readendpos > this.file_end_pos) { // read b[] part in file
                //len = (int)(this.length() - this.current_pos + 1); //wrong pointer
            	len = (int)(this.file_end_pos - this.current_pos + 1); 
            	if(len <=0)
            		return -1;//end of the file, nothing can be read
            } 
            
            this.mbb = file_channel.map(FileChannel.MapMode.READ_ONLY, this.current_pos, len);  
            val = mbb.getLong(); 
    		UnMap();
           
            readendpos = this.current_pos + len - 1;
        }
        this.seek(readendpos + 1);
        return val; 
    }  

}

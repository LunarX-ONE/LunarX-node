/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contactor: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License.
 * The Original Code is:  LunarBase source code 
 * The LunarBase source code is managed by the development team at Lunarion.com.
 * The Initial Developer of the Original Code is the development team at Lunarion.com.
 * Portions created by lunarion are Copyright (C) lunarion.
 * All Rights Reserved.
 *******************************************************************************
 * 
 */

package LCG.StorageEngin.IO.L0;

import java.io.IOException;
 

public interface IOInterface { 
	
	abstract public void init(String name, String mode, int bufbitlen) throws IOException;
	    //abstract public void flushbuf() throws IOException;
	    //abstract public int fillbuf() throws IOException; 
	    //abstract boolean inBuff(long pos);
	    //abstract boolean inBuff(int pos);
		/*
		 * the read() absent the parameter is defined by RandomAccessFile in JDK,
		 * and not allowed to be overload. So here we do not define
		 * it, using read(long pos) instead.
		 */
		//abstract public byte read() throws IOException;
	    abstract public byte read(long pos) throws IOException;
	    abstract public byte read(int pos) throws IOException;
	    abstract public boolean write(byte bw) throws IOException; 
	    abstract public boolean append(byte bw) throws IOException;
	    abstract public boolean write(byte bw, long pos) throws IOException;
	    abstract public boolean write(byte bw, int pos) throws IOException;
	    abstract public long GetCurrentPos(); 
	    abstract public void write(byte b[], int off, int len) throws IOException;
	    abstract public int read(byte b[], int off, int len) throws IOException;
	    abstract public void write(byte b[]) throws IOException; 
	    abstract public int read(byte b[]) throws IOException; 
	    abstract public int readByte(byte b[]) throws IOException; 
	    abstract public void seek(long pos) throws IOException;
	    abstract public void seek(int pos) throws IOException;
	    
	    /*
	     * the following three functions seeks or reports the data end position and 
	     * the data length for real data that flushed in this file.
	     * 
	     * The file expansion is by growGreedy, each time expend by 4096 bytes, 
	     * which improves the IO efficiency, since we do not increase the file length 
	     * by tens of bytes each time slowing down the file system operation.
	     */
	    abstract public void seekEnd() throws IOException;
	    abstract public long length();
	    //abstract public void setLength(long newLength) throws IOException; 
	    
	    
	    abstract public long getFilePointer() throws IOException; 
	    abstract public void flush() throws IOException;  
	    abstract public void flushSync() throws IOException;  
	    abstract public void close() throws IOException; 
	    
	    abstract public void write4ByteInt(int i) throws IOException;
	    abstract public int read4ByteInt() throws IOException;
	    abstract public void WriteLong(long i) throws IOException;
	    abstract public long ReadLong() throws IOException;
	    
	    abstract public void readChars(char[] content, int i, int length)throws IOException;
	    abstract public void writeChars(char[] s, int start, int length) throws IOException;
	    abstract public void writeChars(String s, int start, int length) throws IOException;
	    abstract public int read1ByteInt() throws IOException;
	    abstract public void write1ByteInt(int i) throws IOException;
	    
	    abstract public boolean isNative();
	    
	    abstract public int growGreedy(long current_used_len, int obj_size_needs_append) ;
		
}

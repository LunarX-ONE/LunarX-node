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

package LCG.StorageEngin.IO.L0;

import java.io.File;
import java.io.IOException;

import LCG.MemoryIndex.DefaultNativePath;
 

public class IOChannelMBBNative64 implements IOInterface{ 
    static {
    	try{ 
    		//System.loadLibrary("Linux_X86_32_IOL0_Native");
    		String abs_path = new File (".").getCanonicalPath();
    		
    		System.out.println( "Native lib path is: " + abs_path );
    		System.load(abs_path + "/libLinux_X86_64_IOL0_LLHT.so");
    		//System.load("/home/feiben/eclipsWorkspace/StorageEnginCPP/Debug/libLinux_X86_32_IOL0_Native.so");
    	} catch(UnsatisfiedLinkError e) 
    	{ 
            System.err.println( "Cannot load Linux_X86_64_IOL0_LLHT library:\n " + 
            		e.toString() );
            
            System.out.println("Trying load native lib at: " + DefaultNativePath.final_native_lib_location );
            System.load(DefaultNativePath.final_native_lib_location + "libLinux_X86_64_IOL0_LLHT.so"); 
        
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }
    protected long _physical_mem_addr;
    private boolean closed;
    private boolean terminated;
    private String _name;
    
     

    public IOChannelMBBNative64(String name, String mode, int bufbitlen) throws IOException 
    { 
    	init(name, mode, bufbitlen);
    	_name = name;
    	closed = false;
    	terminated = false;
    } 
    protected void terminate() 
    { 
    	if(terminated)
    		return;
    	
        try { 
        	terminator(_physical_mem_addr); 
        	terminated = true;
        } finally { 
            try { 
                super.finalize(); 
            } catch (Throwable e) { 
                // TODO Auto-generated catch block 
                e.printStackTrace(); 
            } 
        } 
    } 
    
    public void close()
    {
    	if(closed)
    		return;
    	
    	closeMBB(_physical_mem_addr);
    	terminate();
    	closed = true;
    }
    
    public native long initMBB(String name, String mode, int bufbitlen); 
    public native void closeMBB(long physical_mem_addr);
    private native void terminator(long physical_mem_addr);

    public native boolean	isReadOnly(long physical_mem_addr); 
    public native boolean	flush(long physical_mem_addr);
    public native boolean	flushSync(long physical_mem_addr);
    /*
     * flushbuf is an internal call of C++ side, which is 
     * implemented as inline function for performance concerns.
     * For outer invocation, call flush as its equal.
     */
    //public native boolean	flushbuf(int physical_mem_addr);
    public native long		GetCurrentPos(long physical_mem_addr);
    public native boolean	seek(long physical_mem_addr, long pos);
    public native long		length(long physical_mem_addr);
    public native boolean	seekEnd(long physical_mem_addr);
    public native boolean	setLength(long physical_mem_addr, long new_length);
    public native int		readByteArray(long physical_mem_addr, byte b[], int off, int len);
    public native boolean	writeByteArray(long physical_mem_addr, byte b[], int off, int len);
    public native int		readIntArray(long physical_mem_addr, int b[], int off, int len);
    public native boolean	writeIntArray(long physical_mem_addr, int b[], int off, int len);
    
    private native byte		readByte(long physical_mem_addr);
    private native byte		readByteAt(long physical_mem_addr, long pos);
    public native boolean	writeByte(long physical_mem_addr, byte bw);
    private native boolean	writeByteAt(long physical_mem_addr, byte bw, long pos);
	
    /*
     * variable utility functions
     */
    public native boolean	WriteLong(long physical_mem_addr, long i);
    public native long		ReadLong(long physical_mem_addr);
    public native boolean	write1ByteInt(long physical_mem_addr, int i);
    public native int		read1ByteInt (long physical_mem_addr);
    public native boolean	write4ByteInt(long physical_mem_addr, int i);
    public native int		read4ByteInt(long physical_mem_addr);
    
    public native int		growGreedy(long physical_mem_addr, long current_used_len, int obj_size_needs_append);
	
    @Override
	public void init(String name, String mode, int bufbitlen) throws IOException {
		_physical_mem_addr = initMBB(name, mode, bufbitlen);
		
	}
	 
    
	public byte read() throws IOException { 
		return readByte(_physical_mem_addr);
	}  
    
	@Override
	public byte read(long pos) throws IOException {
		return readByteAt(_physical_mem_addr, pos);
	}
	@Override
	public byte read(int pos) throws IOException {
		return read((long)pos);
	}
	@Override
	public boolean write(byte bw) throws IOException {
		return writeByte(_physical_mem_addr, bw);
	}
	@Override
	public boolean append(byte bw) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean write(byte bw, long pos) throws IOException {
		 
		return writeByteAt(_physical_mem_addr, bw, pos);
	}
	@Override
	public boolean write(byte bw, int pos) throws IOException {
		return write(bw, (long)pos);
	}
 
	public long GetCurrentPos() {
		 
		return GetCurrentPos(_physical_mem_addr);
	}
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		writeByteArray(_physical_mem_addr, b, off, len); 
	}
	public void write(int[] b, int off, int len)
	{
		writeIntArray(_physical_mem_addr, b, off, len); 
	}
	@Override
	public int read(byte[] b, int off, int len) throws IOException { 
		return readByteArray(_physical_mem_addr, b, off, len);
	}
	public int read(int[] b, int off, int len) throws IOException { 
		return readIntArray(_physical_mem_addr, b, off, len);
	}
	@Override
	public void write(byte[] b) throws IOException {
		writeByteArray(_physical_mem_addr, b, 0, b.length);
		
	}
	@Override
	public int read(byte[] b) throws IOException { 
		return readByteArray(_physical_mem_addr, b, 0, b.length);
	}
	@Override
	public int readByte(byte[] b) throws IOException {
		 return readByteArray(_physical_mem_addr, b,0,1);
	}
	@Override
	public void seek(long pos) throws IOException {
		seek(_physical_mem_addr, pos);
		
	}
	@Override
	/*
	 * (non-Javadoc)
	 * @see LCG.StorageEngin.IO.L0.IOInterface#seek(int)
	 */
	/*
	 * seek calls native interface that expand the file length. 
	 * Since if seek a wrong place will greatly expand the file where there is 
	 * no data exists, the length() will be wrong.
	 * 
	 * If application use length() to calculate the existing element in 
	 * the file, use seek carefully. 
	 */
	public void seek(int pos) throws IOException {
		seek(_physical_mem_addr, (long)pos);
		
	}
	@Override
	public void seekEnd() throws IOException {
		seekEnd(_physical_mem_addr); 
	}
	@Override
	public long length() {
		 
		return length(_physical_mem_addr);
	}
	//@Override
	//public void setDataLength(long newLength) throws IOException {
	//	setLength(_physical_mem_addr, newLength);
		
	//}
	@Override
	public long getFilePointer() throws IOException {
		 
		return GetCurrentPos(_physical_mem_addr);
	}
	@Override
	public void flush() throws IOException {
		flush(_physical_mem_addr);
		
	}
	@Override
	public void flushSync() throws IOException {
		flushSync(_physical_mem_addr);
		
	}
	
	@Override
	public void write4ByteInt(int i) throws IOException {
		/*
		 * implemented by derived class IOStreamNative
		 */
		
	}
	@Override
	public int read4ByteInt() throws IOException {
		/*
		 * implemented by derived class IOStreamNative
		 */
		return 0;
	}
	@Override
	public void WriteLong(long i) throws IOException {
		/*
		 * implemented by derived class IOStreamNative
		 */
		
	}
	@Override
	public long ReadLong() throws IOException {
		/*
		 * implemented by derived class IOStreamNative
		 */
		return 0;
	}
	@Override
	public void readChars(char[] content, int i, int length) throws IOException {
		/*
		 * implemented by derived class IOStream
		 */
	}
	@Override
	public void writeChars(char[] s, int start, int length) throws IOException {
		/*
		 * implemented by derived class IOStream
		 */
	}
	
	@Override
	public void writeChars(String s, int start, int length) throws IOException {
		/*
		 * implemented by derived class IOStream
		 */
	}
	@Override
	public int read1ByteInt() throws IOException {
		/*
		 * implemented by derived class IOStream
		 */
		return 0;
	}
	@Override
	public void write1ByteInt(int i) throws IOException {
		/*
		 * implemented by derived class IOStream
		 */
		
	}
	@Override
	public boolean isNative() {
		return true;
		
	}
	
	public int growGreedy(long current_used_len, int obj_size_needs_append) 
	{
		return growGreedy(_physical_mem_addr, current_used_len, obj_size_needs_append);
		
	}
	
}

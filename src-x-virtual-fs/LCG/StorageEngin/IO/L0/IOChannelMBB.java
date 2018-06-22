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

import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ResourceBundle; 
 
 

public class IOChannelMBB extends RandomAccessFile implements IOInterface {

    static ResourceBundle res = ResourceBundle.getBundle("LCG.StorageEngin.IO.L0.Res");
    private static final int DEFAULT_BUFFER_BIT_LEN = 10;
     
 
    protected byte[] buff;
    protected int buff_bit_length;
    protected int buff_size;
    protected long bufmask;
    protected boolean if_buff_dirty;
    protected int buff_used_size;
    protected long current_pos;

    protected long buff_start_pos;
    protected long buff_end_pos;
    protected long file_end_pos;

    protected boolean append;
    protected boolean read_only;
    public String file_name;
    public long init_file_len; 
    
    protected FileChannel file_channel;
    protected MappedByteBuffer mbb = null;  
    private int map_count;
    private final int MAX_MAP_COUNT = 50;
    

    public IOChannelMBB(String name) throws  IOException {
        this(name, res.getString("r"), DEFAULT_BUFFER_BIT_LEN);
    }

    public IOChannelMBB(File file) throws IOException, FileNotFoundException {
        this(file.getPath(), res.getString("r"), DEFAULT_BUFFER_BIT_LEN);
    }

    public IOChannelMBB(String name, int bufbitlen) throws  IOException {
        this(name, res.getString("r"), bufbitlen);
    }

    public IOChannelMBB(File file, int bufbitlen) throws IOException, FileNotFoundException {
        this(file.getPath(), res.getString("r"), bufbitlen);
    }

    public IOChannelMBB(String name, String mode) throws IOException {
        this(name, mode, DEFAULT_BUFFER_BIT_LEN);
    }

    public IOChannelMBB(File file, String mode) throws IOException, FileNotFoundException {
        this(file.getPath(), mode, DEFAULT_BUFFER_BIT_LEN);
    }

    public IOChannelMBB(String name, String mode, int bufbitlen) throws IOException  {
        super(name, mode);
        this.init(name, mode, bufbitlen);
    }

    public IOChannelMBB(File file, String mode, int bufbitlen) throws IOException, FileNotFoundException {
        this(file.getPath(), mode, bufbitlen);
    }

    public void init(String name, String mode, int bufbitlen) throws IOException {
        if (mode.equals(res.getString("r")) == true) {
            this.append = false;
            this.read_only = true;
        } else {
            this.append = true;
            this.read_only = false;
        }

        this.file_name = name;
        this.init_file_len = super.length();
        this.file_end_pos = this.init_file_len - 1;
 
        if (bufbitlen < 0) {
            throw new IllegalArgumentException(res.getString("bufbitlen_size_must_0"));
        }

        this.buff_bit_length = bufbitlen;
        this.buff_size = 1 << bufbitlen;
        
        //this.buff = ByteBuffer.allocate(this.buff_size);
        this.buff = new byte[this.buff_size];
        this.bufmask = ~((long)this.buff_size - 1L);
        this.if_buff_dirty = false;
        this.buff_used_size = 0;
        this.buff_start_pos = -1;
        this.buff_end_pos = -1; 
        
        this.file_channel = this.getChannel(); 
        this.mbb = null;
        this.map_count = 0;
         
    }
    
    //the offcial provides no unmap solution, we can only count on system.gc()
    //shit!
    protected void UnMap()
    {
    	this.map_count++;
		if(this.map_count % this.MAX_MAP_COUNT == this.MAX_MAP_COUNT -1)
		{
			this.map_count = 0;
			mbb = null;
			System.gc();
		}
    }

    public boolean isReadOnly()
    {
    	return this.read_only;
    }
    
	private void flushbuf() throws IOException {
        if (this.if_buff_dirty == true) {
            if (this.file_channel.position() != this.buff_start_pos) {
                this.file_channel.position(this.buff_start_pos); 
            } 
             
            mbb = file_channel.map(FileChannel.MapMode.READ_WRITE, this.buff_start_pos, this.buff_used_size);  
    		if(buff.length < buff_used_size)
    		{
    			buff_used_size = buff.length;
    		}
            mbb.put(buff, 0, buff_used_size);
    		UnMap();
    		
    		this.buff_used_size = 0;
    	    this.if_buff_dirty = false;
        }
    }

	public int fillbuf() throws IOException {
     
        this.if_buff_dirty = false; 
        
        int fill_length = (int) this.min(this.buff.length, this.length()-this.buff_start_pos);
        if(fill_length <=0)
        	return 0;
        
        UnMap();
        this.mbb = file_channel.map(FileChannel.MapMode.READ_ONLY, this.buff_start_pos, fill_length);  
		mbb.get(this.buff, 0, fill_length);  
		
		
		this.if_buff_dirty = false;
		return fill_length;
	 
    }

	public boolean inBuff(long position)
    {
    	if(position >= this.buff_start_pos && position <= this.buff_end_pos)
    		return true;
    	return false;
    }
	
	 
	
    public byte read(long pos) throws IOException {
    	 if (!inBuff(pos)) 
    	 {
             this.flushbuf();
             this.seek(pos);

             if ((pos < this.buff_start_pos) || (pos > this.buff_end_pos)) {
                 throw new IOException();
             }
         }
         this.current_pos = pos + 1; 
         return this.buff[(int)(pos - this.buff_start_pos)];
    	
    	 
    }

    public boolean write(byte bw) throws IOException {
        return this.write(bw, this.current_pos);
    }  
    
    public boolean append(byte bw) throws IOException {
        return this.write(bw, this.file_end_pos + 1);
    }

    public boolean write(byte bw, long pos) throws IOException {

        if (this.inBuff(pos)) 
        {  
        	this.buff[(int)(pos - this.buff_start_pos)] = bw;
        	this.if_buff_dirty = true;
            
            if (pos == this.file_end_pos + 1) {  
                this.file_end_pos++; 
            }
        } else {  
            this.seek(pos);

            if ((pos >= 0) && (pos <= this.file_end_pos) && (this.file_end_pos != 0)) {  
                this.buff[(int)(pos - this.buff_start_pos)] = bw;
            	
            } else if (((pos == 0) && (this.file_end_pos == 0)) || (pos == this.file_end_pos + 1)) { // write pos is append pos
            	this.buff[0] = bw; 
                this.file_end_pos++; 
            } else {
                throw new IndexOutOfBoundsException();
            }
            this.if_buff_dirty = true;
        } 
        
        if(this.buff_used_size < (int)(pos - this.buff_start_pos + 1) )
        	this.buff_used_size = (int)(pos - this.buff_start_pos + 1);
        
        this.current_pos = pos+1;
        return true;
    }

    public long GetCurrentPos()
    {
    	return this.current_pos;
    }
    
 
    public void write(ByteBuffer byte_buffer) throws IOException {
    	if(byte_buffer == null)
    		return;
    	
    	mbb = file_channel.map(FileChannel.MapMode.READ_WRITE, this.current_pos, byte_buffer.capacity());    	
		mbb.put(byte_buffer); 
		UnMap();
		this.seek(this.current_pos + byte_buffer.capacity());
		
    }
    public void write(byte b[], int off, int len) throws IOException {

    	long write_end_pos = this.current_pos + len - 1;

        if (write_end_pos <= this.buff_end_pos) {  
        	System.arraycopy(b, off, this.buff, (int)(this.current_pos - this.buff_start_pos), len);
  
            this.if_buff_dirty = true;
            if(this.buff_used_size < (int)(write_end_pos - this.buff_start_pos + 1) )
            	this.buff_used_size = (int)(write_end_pos - this.buff_start_pos + 1);
  
        } else { // b[] not in cur buf 
        	mbb = file_channel.map(FileChannel.MapMode.READ_WRITE, this.current_pos, len);  
    		mbb.put(b); 
    		UnMap();
        }

        if (write_end_pos > this.file_end_pos)
            this.file_end_pos = write_end_pos;

        this.seek(write_end_pos+1); 
    }

    public ByteBuffer Map(int len) throws IOException
    {
    	long readendpos = this.current_pos + len - 1;

    	ByteBuffer t_b_b;
        if (readendpos <= this.buff_end_pos && readendpos <= this.file_end_pos ) { // read in buf
            //System.arraycopy(this.buff, (int)(this.current_pos - this.buff_start_pos), b, off, len);
        	t_b_b = ByteBuffer.wrap(this.buff, (int)(this.current_pos - this.buff_start_pos), len);
        } else { 
        	// read b[] size > buf[]
            if (readendpos > this.file_end_pos) { // read b[] part in file
                //len = (int)(this.length() - this.current_pos + 1); //wrong pointer
            	len = (int)(this.file_end_pos - this.current_pos + 1); 
            	if(len <=0)
            		return null;//end of the file, nothing can be read
            } 
            
            this.mbb = file_channel.map(FileChannel.MapMode.READ_ONLY, this.current_pos, len);  
            t_b_b = mbb.slice();
    		UnMap();
           
            readendpos = this.current_pos + len - 1;
        }
        this.seek(readendpos + 1);
        return t_b_b;
    }
    public int read(byte b[], int off, int len) throws IOException {

        long readendpos = this.current_pos + len - 1;

        if (readendpos <= this.buff_end_pos && readendpos <= this.file_end_pos ) { // read in buf
            System.arraycopy(this.buff, (int)(this.current_pos - this.buff_start_pos), b, off, len);
        } else { 
        	// read b[] size > buf[]
            if (readendpos > this.file_end_pos) { // read b[] part in file
                //len = (int)(this.length() - this.current_pos + 1); //wrong pointer
            	len = (int)(this.file_end_pos - this.current_pos + 1); 
            	if(len <=0)
            		return -1;//end of the file, nothing can be read
            } 
            
            this.mbb = file_channel.map(FileChannel.MapMode.READ_ONLY, this.current_pos, len);  
            mbb.get(b, off, len); 
    		UnMap();
           
            readendpos = this.current_pos + len - 1;
        }
        this.seek(readendpos + 1);
        return len;
    }

    public void write(byte b[]) throws IOException {
        this.write(b, 0, b.length);
    }

    public int read(byte b[]) throws IOException {
        return this.read(b, 0, b.length);
    }
    
    public int readByte(byte b[]) throws IOException {
    	if(b.length>1)
    		return -1;
    	if(read(b)!=-1)
        { 
    		return 1;
        }
    	else
    		return -1;
    }
    

    /*
     * if pos is bigger than file_end_pos,
     * set file_end_pos = pos-1, which means expand the file.
     */
    public void seek(long pos) throws IOException {

    	if (!this.inBuff(pos))
    	{  
            this.flushbuf();

            if ((pos >= 0) && (pos <= this.file_end_pos) && (this.file_end_pos != 0)) { // seek pos within file (file length > 0)
                this.buff_start_pos =  pos & this.bufmask; 
                this.buff_used_size = this.fillbuf();

            } else if (((pos == 0) && (this.file_end_pos == 0)) || (pos == this.file_end_pos + 1)) { // seek pos is append pos

                this.buff_start_pos = pos;
                this.buff_used_size = 0; 
            }
            //in case need to seek a place away from file end.
            //in a block file system, always write into a block and left 
            //a hole before the end of the block, say, 3 bytes, 
            //then the file end position has 3 bytes distance away 
            //from the block end. we need to seek to the block end 
            //to write another block
            else
            {
            	this.file_end_pos = pos - 1;
            	this.buff_start_pos = pos;
                this.buff_used_size = 0; 
            }
            this.buff_end_pos = this.buff_start_pos + this.buff_size - 1;
        }
        this.current_pos = pos; 
        if(pos >= this.file_end_pos+1)
    		this.file_end_pos = pos-1;
 
    }

    public void seekEnd() throws IOException
    {
    	long end = this.length();
    	this.seek(end);
    }
    
    public long length() {
        return this.max(this.file_end_pos + 1, this.init_file_len);
    }

    public void setDataLength(long newLength) throws IOException {
        if (newLength > 0) {
            this.file_end_pos = newLength - 1;
        } else {
            this.file_end_pos = 0;
        }
        super.setLength(newLength);
    }
    
    public long getFilePointer() throws IOException {
        return this.file_channel.position();
    }

    private long max(long a, long b) {
        if (a > b) return a;
        return b;
    }
    
    private long min(long a, long b) {
        if (a < b) return a;
        return b;
    }

    //must run before close
    public void flush() throws IOException
    {
    	this.flushbuf();
    }
    
    public void close() throws IOException {
 
        //this.mbb.force();
        UnMap();
        //this.file_channel.close();//calls randomaccess.close again, thus calls .flushbuf() and .force again
        super.close(); 
    }

 
	public boolean inBuff(int position) {
		return this.inBuff((long)position);
	}

	@Override
	public byte read(int pos) throws IOException {
		return this.read((long)pos);
	}

	@Override
	public boolean write(byte bw, int pos) throws IOException {
		return this.write(bw, (long)pos); 
	}

	@Override
	public void seek(int pos) throws IOException {
		this.seek((long)pos);
		
	}

	@Override
	public void write4ByteInt(int i) throws IOException {
		/*
		 * implemented by derived class IOStream
		 */
		
	}

	@Override
	public int read4ByteInt() throws IOException {
		/*
		 * implemented by derived class IOStream
		 */
		return 0;
	}

	@Override
	public void WriteLong(long i) throws IOException {
		/*
		 * implemented by derived class IOStream
		 */
		
	}

	@Override
	public long ReadLong() throws IOException {
		/*
		 * implemented by derived class IOStream
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
		return false;
	}

	@Override
	public int growGreedy(long current_used_len, int obj_size_needs_append) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void writeChars(String s, int start, int length) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flushSync() throws IOException {
		// TODO Auto-generated method stub
		
	}

   
}

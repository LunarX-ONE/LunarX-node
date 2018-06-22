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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ResourceBundle; 
 

//this implementation is based on an Internet article published on IBM developerworks,
//which greatly improves the IO operation.
//see the original article:
//http://www.ibm.com/developerworks/cn/java/l-javaio/index.html
//And it seem performing better than MappedByteBuffer as NIO provides
//Note that the original code from above link has some bugs. 
//Here we have them fixed.

@Deprecated
public class IOBuffered extends RandomAccessFile {

    static ResourceBundle res = ResourceBundle.getBundle("LCG.StorageEngin.IO.Res");
    private static final int DEFAULT_BUFFER_BIT_LEN = 10;
     
    protected byte buff[];
    //protected ByteBuffer buff;
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
    public String file_name;
    public long init_file_len; 
    

    public IOBuffered(String name) throws  IOException {
        this(name, res.getString("r"), DEFAULT_BUFFER_BIT_LEN);
    }

    public IOBuffered(File file) throws IOException, FileNotFoundException {
        this(file.getPath(), res.getString("r"), DEFAULT_BUFFER_BIT_LEN);
    }

    public IOBuffered(String name, int bufbitlen) throws  IOException {
        this(name, res.getString("r"), bufbitlen);
    }

    public IOBuffered(File file, int bufbitlen) throws IOException, FileNotFoundException {
        this(file.getPath(), res.getString("r"), bufbitlen);
    }

    public IOBuffered(String name, String mode) throws IOException {
        this(name, mode, DEFAULT_BUFFER_BIT_LEN);
    }

    public IOBuffered(File file, String mode) throws IOException, FileNotFoundException {
        this(file.getPath(), mode, DEFAULT_BUFFER_BIT_LEN);
    }

    public IOBuffered(String name, String mode, int bufbitlen) throws IOException  {
        super(name, mode);
        this.init(name, mode, bufbitlen);
    }

    public IOBuffered(File file, String mode, int bufbitlen) throws IOException, FileNotFoundException {
        this(file.getPath(), mode, bufbitlen);
    }

    private void init(String name, String mode, int bufbitlen) throws IOException {
        if (mode.equals(res.getString("r")) == true) {
            this.append = false;
        } else {
            this.append = true;
        }

        this.file_name = name;
        this.init_file_len = super.length();
        this.file_end_pos = this.init_file_len - 1;
        this.current_pos = super.getFilePointer();

        if (bufbitlen < 0) {
            throw new IllegalArgumentException(res.getString("bufbitlen_size_must_0"));
        }

        this.buff_bit_length = bufbitlen;
        this.buff_size = 1 << bufbitlen;
        this.buff = new byte[this.buff_size];
 
        this.bufmask = ~((long)this.buff_size - 1L);
        this.if_buff_dirty = false;
        this.buff_used_size = 0;
        this.buff_start_pos = -1;
        this.buff_end_pos = -1; 
         
    }

    private void flushbuf() throws IOException {
        if (this.if_buff_dirty == true) {
            if (super.getFilePointer() != this.buff_start_pos) {
                super.seek(this.buff_start_pos);
                
            }
            super.write(this.buff, 0, this.buff_used_size); 
             
    	    this.if_buff_dirty = false;
        }
    }

    private int fillbuf() throws IOException {
        super.seek(this.buff_start_pos);
        this.if_buff_dirty = false;
        return super.read(this.buff);
        //while using file channel, the buff must clear first
        //this.buff.clear();
        //return file_channel.read(this.buff);
        
    }

    public byte read(long pos) throws IOException {
        if (pos < this.buff_start_pos || pos > this.buff_end_pos) {
            this.flushbuf();
            this.seek(pos);

            if ((pos < this.buff_start_pos) || (pos > this.buff_end_pos)) {
                throw new IOException();
            }
        }
        this.current_pos = pos; 
        return this.buff[(int)(pos - this.buff_start_pos)];
    }

    public boolean write(byte bw) throws IOException {
        return this.write(bw, this.current_pos);
    }  
    
    public boolean append(byte bw) throws IOException {
        return this.write(bw, this.file_end_pos + 1);
    }

    public boolean write(byte bw, long pos) throws IOException {

        if ((pos >= this.buff_start_pos) && (pos <= this.buff_end_pos)) { // write pos in buf
 
        	this.buff[(int)(pos - this.buff_start_pos)] = bw;
        	this.if_buff_dirty = true;
            
            if (pos == this.file_end_pos + 1) { // write pos is append pos
                this.file_end_pos++;
                //  fff  this.buff_used_size++;
            }
        } else { // write pos not in buf
            this.seek(pos);

            if ((pos >= 0) && (pos <= this.file_end_pos) && (this.file_end_pos != 0)) { // write pos is modify file
                this.buff[(int)(pos - this.buff_start_pos)] = bw;
            	
            } else if (((pos == 0) && (this.file_end_pos == 0)) || (pos == this.file_end_pos + 1)) { // write pos is append pos
            	this.buff[0] = bw; 
                this.file_end_pos++;
                //  fff this.buff_used_size = 1;
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
    public void write(byte b[], int off, int len) throws IOException {

        long write_end_pos = this.current_pos + len - 1;

        if (write_end_pos <= this.buff_end_pos) { // b[] in cur buf
            System.arraycopy(b, off, this.buff, (int)(this.current_pos - this.buff_start_pos), len);
       
            this.if_buff_dirty = true;
            if(this.buff_used_size < (int)(write_end_pos - this.buff_start_pos + 1) )
            	this.buff_used_size = (int)(write_end_pos - this.buff_start_pos + 1);
  
        } else { // b[] not in cur buf
            super.seek(this.current_pos);
            super.write(b, off, len); 
        }

        if (write_end_pos > this.file_end_pos)
            this.file_end_pos = write_end_pos;

        this.seek(write_end_pos+1);
    }

    public int read(byte b[], int off, int len) throws IOException {

        long readendpos = this.current_pos + len - 1;

        if (readendpos <= this.buff_end_pos && readendpos <= this.file_end_pos ) { // read in buf
            System.arraycopy(this.buff, (int)(this.current_pos - this.buff_start_pos), b, off, len);
        	//System.arraycopy(this.buff.array(), (int)(this.current_pos - this.buff_start_pos), b, off, len);
        } else { // read b[] size > buf[]

            if (readendpos > this.file_end_pos) { // read b[] part in file
                //len = (int)(this.length() - this.current_pos + 1);//wrong pointer
            	len = (int)(this.file_end_pos - this.current_pos + 1);
            }

            super.seek(this.current_pos);
            len = super.read(b, off, len);
           
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
    

    //seek the position in file and fill the buffer with the data begin at this position
    public void seek(long pos) throws IOException {

        if ((pos < this.buff_start_pos) || (pos > this.buff_end_pos)) { // seek pos not in buf
            this.flushbuf();

            if ((pos >= 0) && (pos <= this.file_end_pos) && (this.file_end_pos != 0)) { // seek pos within file (file length > 0)
                this.buff_start_pos =  pos & this.bufmask;
                this.buff_used_size = this.fillbuf();

            } else if (((pos == 0) && (this.file_end_pos == 0)) || (pos == this.file_end_pos + 1)) { // seek pos is append pos

                this.buff_start_pos = pos;
                this.buff_used_size = 0;
            }
            this.buff_end_pos = this.buff_start_pos + this.buff_size - 1;
        }
        this.current_pos = pos;
    }

    public void seekEnd() throws IOException
    {
    	long end = this.length();
    	seek(end);
    }
    
    public long length() throws IOException {
        return this.max(this.file_end_pos + 1, this.init_file_len);
    }

    public void setLength(long newLength) throws IOException {
        if (newLength > 0) {
            this.file_end_pos = newLength - 1;
        } else {
            this.file_end_pos = 0;
        }
        super.setLength(newLength);
    }
    
    public long getFilePointer() throws IOException {
        return this.current_pos;
    }

    private long max(long a, long b) {
        if (a > b) return a;
        return b;
    }

    public void flush() throws IOException
    {
    	this.flushbuf();
    }
    public void close() throws IOException {
  
        super.close(); 
    }

   
}

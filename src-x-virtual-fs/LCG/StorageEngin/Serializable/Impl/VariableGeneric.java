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



package LCG.StorageEngin.Serializable.Impl;  
import java.io.IOException;   
import LCG.StorageEngin.IO.L0.IOInterface; 
 
//This is a class dealing with some useful variables,
//and it can write to memory store or real disk file,
//which depends on the generic type the user choose.
public class VariableGeneric<H extends IOInterface>{   

	H l_inner_stream;  
	
	public VariableGeneric(H stream_implementation) throws IOException  {
		l_inner_stream = stream_implementation;
    }
	
	public void write1ByteInt(int i) throws IOException {
		byte[] byte_buff = new byte[1]; 
    	byte_buff[0]= (byte) i;
    	l_inner_stream.write(byte_buff, 0, 1);   
    }
	
	public int read1ByteInt() throws IOException {
		byte[] byte_buff = new byte[1];
        if(l_inner_stream.read(byte_buff)!=-1)
        {
        	return ((int)byte_buff[0])& 0xFF; 
        }
        else
        	return -1; 
    }  
	
    /*
  	 * low efficient
  	 */
    @Deprecated
    public final void write4ByteInt(int i) throws IOException { 
    	byte[] byte_buff = new byte[4];
    	byte_buff[0]= (byte) (i >> 24);
    	byte_buff[1]= (byte) (i >> 16);
    	byte_buff[2]= (byte) (i >> 8);
    	byte_buff[3]= (byte) i;
    	l_inner_stream.write(byte_buff, 0, 4);
    	 
    }
    
  	/*
  	 * low efficient
  	 */
    @Deprecated
    public final int read4ByteInt() throws IOException { 
        byte[] byte_buff = new byte[4];
        if(l_inner_stream.read(byte_buff)!=-1)
        {
        	return Transform4ByteToInt(byte_buff, 0);
        }
        else
        	return -1;
        	  
    }
    
    static public final void TransformIntTo4Byte(byte[] b, int from, int i)
    {
    	b[from]= (byte) (i >> 24);
    	b[from+1]= (byte) (i >> 16);
    	b[from+2]= (byte) (i >> 8);
    	b[from+3]= (byte) i; 
    }
    
    
    static public final int Transform4ByteToInt(byte[] b, int from)
    {
    	if((from+3)>=b.length || b==null)
    		return -1;
    	else
    		return  ((b[from]& 0xFF) << 24) | ((b[from+1] & 0xFF) << 16)
    			| ((b[from+2] & 0xFF) << 8) | (b[from+3] & 0xFF);

    }
    
    static public final long Transform8ByteToLong(byte[] b, int from)
    {
    	if((from+7)>=b.length || b==null)
    		return -1;
    	else
    		return (((long)Transform4ByteToInt(b, from))<< 32) | (Transform4ByteToInt(b, from+4) & 0xFFFFFFFFL);
    		 
    }
	
    /**
     * this implementation is from the lucene document, 
     * which designs the variant length of Int:
     * A variable-length format for positive integers is defined 
     * where the high-order bit of each byte indicates whether more bytes 
     * remain to be read. The low-order seven bits are appended as 
     * increasingly more significant bits in the resulting integer value. 
     * Thus values from zero to 127 may be stored in a single byte, 
     * values from 128 to 16,383 may be stored in two bytes, and so on.
     * 
     *  
     * But be noticed, it is about 3 times slower than fixed length int read 
     * and write. For a 36M file, it takes 1s to write and almost 2s to read.
     * But when we use fixed length int read like read4ByteInt,
     * it takes only 0.5s to finish.
     * 
     * Writes an int in a variable-length format. Writes between one and five
     * bytes. Smaller values take fewer bytes. Negative numbers are not
     * supported.
     * 
     * @see VInputStream#readVInt()
     */
    public final void writeVInt(int i) throws IOException {
        while ((i & ~0x7F) != 0) {
        	l_inner_stream.write((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        l_inner_stream.write((byte) i);
    }
    
    /**Read follows the same logic Write does.
     * 
     * But be noticed, it is about 3 times slower than fixed length int read 
     * and write. For a 36M file, it takes 1s to write and almost 2s to read.
     * But when we use fixed length int read like read4ByteInt,
     * it takes only 0.5s to finish.
     * 
     * Reads an int stored in variable-length format. Reads between one and five
     * bytes. Smaller values take fewer bytes. Negative numbers are not
     * supported.
     * 
     * @see VOutputStream#writeVInt(int)
     */
    public final int readVInt() throws IOException {
        byte[] b = new byte[1];
        if(l_inner_stream.read(b)!=-1)
        { 
        	int i = b[0] & 0x7F;
        	for (int shift = 7; (b[0] & 0x80) != 0; shift += 7) {
        		l_inner_stream.read(b);
        		i |= (b[0] & 0x7F) << shift;
        	}
        	return i;
        }
        else
        	return -1;
    }
    
    /*public final void write256CharsUTF8String(String s) throws IOException {
    	//means whatever the input string is, 
    	//the length of string is cut to 256 at most
    	int length = s.length();
    	if(length >256 )
    		write1ByteInt(256);
    	else
    		write1ByteInt(length);
        
    	writeChars(s, 0, length);
    }
   
   public final String read256CharsUTF8String() throws IOException {
    	//means the length of string is 256 at most
    	int length = read1ByteInt();
    	char[] stringReadBuffer = new char[length]; 
    	readChars(stringReadBuffer, 0, length);
    	return new String(stringReadBuffer, 0, length);
    }*/
    
    /*
     * ﻿writeChars(char[],int,int)用来把一个符合UTF-8编码的字符数组写入文件，它同样把字符拆分成字节来对待。对每个字符，按照其有效位数n（去掉高位的0）的不同，采用有三种不同的写入方法：
     * (1).0< n <=7，取后7位，首位置0写入文件
     * (2).7< n <=11或者n=0，取高1-5位，首3位置110；取后6位，首2位置10；写入文件
     * (3).11< n <=16,取高0-4位，首4位置1110；取中6位，首2位置10；取后6位，首2位置10；写入文件
    */
    public final void writeChars(String s, int start, int length) throws IOException {
    	 
    	final int end = start + length;
    	for (int i = start; i < end; i++) {    
    		 
    		final int code = (int) s.charAt(i);
    		if (code >= 0x01 && code <= 0x7F)
    			// code值在0x01-0x7F，直接写入
    			// code的有效位数为1-7位
    			l_inner_stream.write((byte) code);
    		else if (((code >= 0x80) && (code <= 0x7FF)) || code == 0) {
    			 // code值在0x80-0x7FF或者为0，则分两个字节写入
    			// code的有效位数8-11位
    			l_inner_stream.write((byte) (0xC0 | (code >> 6)));      // 写高2-5位，首3位置110
    			l_inner_stream.write((byte) (0x80 | (code & 0x3F)));    // 写低6位，首2位置10
    		} 
    		else {
    			 //0x7FF之后的用3个字节写入，code有效位数12-16位
    			l_inner_stream.write((byte) (0xE0 | (code >>> 12)));     // 写高0-4位，首4位置1110
    			l_inner_stream.write((byte) (0x80 | ((code >> 6) & 0x3F)));     //写此高位6位，首2位置10
    			l_inner_stream.write((byte) (0x80 | (code & 0x3F)));        //写低6位，首2位置10
    		}
    	}
    }
    
    public final void writeChars(char[] s, int start, int length) throws IOException {
    	 
    	final int end = start + length;
    	for (int i = start; i < end; i++) {    
    		 
    		final int code = (int) s[i];
    		if (code >= 0x01 && code <= 0x7F)
    			// code值在0x01-0x7F，直接写入
    			// code的有效位数为1-7位
    			l_inner_stream.write((byte) code);
    		else if (((code >= 0x80) && (code <= 0x7FF)) || code == 0) {
    			 // code值在0x80-0x7FF或者为0，则分两个字节写入
    			// code的有效位数8-11位
    			l_inner_stream.write((byte) (0xC0 | (code >> 6)));      // 写高2-5位，首3位置110
    			l_inner_stream.write((byte) (0x80 | (code & 0x3F)));    // 写低6位，首2位置10
    		} 
    		else {
    			//0x7FF之后的用3个字节写入，code有效位数12-16位
    			l_inner_stream.write((byte) (0xE0 | (code >>> 12)));   // 写高0-4位，首4位置1110
    			l_inner_stream.write((byte) (0x80 | ((code >> 6) & 0x3F)));     //写此高位6位，首2位置10
    			l_inner_stream.write((byte) (0x80 | (code & 0x3F)));        //写低6位，首2位置10
    		}
    	}
    }
    
    public final static byte[] utf8Encode(String s, int start, int length) 
    { 
    	final int end = start + length;
    	/*
    	 * calculate byte length.
    	 */
    	int byte_len = 0;
    	for (int i = start; i < end; i++) {    
   		 
    		final int code = (int) s.charAt(i);
    		if (code >= 0x01 && code <= 0x7F)
    			byte_len++;
    		else if (((code >= 0x80) && (code <= 0x7FF)) || code == 0) 
    			byte_len+=2; 
    		else 
    			byte_len+=3; 
    	}
    	byte[] byte_array_encoded = new byte[byte_len];
    	int pos = 0;
    	for (int i = start; i < end; i++) 
    	{ 
    		final int code = (int) s.charAt(i);
    		if (code >= 0x01 && code <= 0x7F)
    		{	 
    			//l_inner_stream.write((byte) code);
    			byte_array_encoded[pos] = (byte) code;
    			pos++;
    		}
    		else if (((code >= 0x80) && (code <= 0x7FF)) || code == 0) {
    			 // code值在0x80-0x7FF或者为0，则分两个字节写入
    			// code的有效位数8-11位
    			//l_inner_stream.write((byte) (0xC0 | (code >> 6)));      // 写高2-5位，首3位置110
    			//l_inner_stream.write((byte) (0x80 | (code & 0x3F)));    // 写低6位，首2位置10
    			byte_array_encoded[pos] = (byte) (0xC0 | (code >> 6));
    			byte_array_encoded[pos+1] = (byte) (0x80 | (code & 0x3F));
    			pos += 2;
    		} 
    		else {
    			 //0x7FF之后的用3个字节写入，code有效位数12-16位
    			//l_inner_stream.write((byte) (0xE0 | (code >>> 12)));     // 写高0-4位，首4位置1110
    			//l_inner_stream.write((byte) (0x80 | ((code >> 6) & 0x3F)));     //写此高位6位，首2位置10
    			//l_inner_stream.write((byte) (0x80 | (code & 0x3F)));        //写低6位，首2位置10
    			byte_array_encoded[pos] = (byte) (0xE0 | (code >>> 12));
    			byte_array_encoded[pos+1] = (byte) (0x80 | ((code >> 6) & 0x3F));
    			byte_array_encoded[pos+2] = (byte) (0x80 | (code & 0x3F));
    			pos += 3;
    		}
    	}
    	return byte_array_encoded;
    }
    public final void readChars(char[] buffer, int start, int length)
    		throws IOException {
    	final int end = start + length;
    	byte[] b = new byte[1];
		byte[] b1 = new byte[1];
		byte[] b2 = new byte[1];
    	for (int i = start; i < end; i++) { 
    		
    		if(l_inner_stream.readByte(b)!=-1)
            { 
    			//如果首位不为1，说明该字节单独为一字符
    			if ((b[0] & 0x80) == 0)  
        			buffer[i] = (char) (b[0] & 0x7F);
    			// 首4位不为1110
        		else if ((b[0] & 0xE0) != 0xE0) {
        			l_inner_stream.readByte(b1);
        			buffer[i] = (char) (((b[0] & 0x1F) << 6) | (b1[0] & 0x3F));
        		} 
        		else {
        			l_inner_stream.readByte(b1);
        			l_inner_stream.readByte(b2);
        			buffer[i] = (char) (((b[0] & 0x0F) << 12)| ((b1[0] & 0x3F) << 6) | (b2[0] & 0x3F));
        		}
        	} 
            else
            	return; 
    	}
    }
    
    /*
     * must start at 0;
     */
    public final static char[] utf8Decode(byte[] source, int byte_length)
	{
    	final int end = Math.min( byte_length, source.length);
    	
		/*
		 * calculate char length.
		 */
    	int pos = 0;
    	int char_len = 0;
		while(pos < end){  
    			//如果首位不为1，说明该字节单独为一字符
    			if ((source[pos] & 0x80) == 0)  
    			{ 
    				char_len += 1;
    				pos += 1;
    			}
    			// 首4位不为1110
        		else if ((source[pos] & 0xE0) != 0xE0) { 
        			char_len += 1;
        			pos += 2;
        		} 
        		else { 
        			char_len += 1;
        			pos += 3;
        		} 
    	}
	 
		if(pos > end)
			char_len--;
		
		char[] char_decoded = new char[char_len];
		pos = 0;
    	for (int i = 0; i < char_len; i++) { 
    			//如果首位不为1，说明该字节单独为一字符
    			if ((source[pos] & 0x80) == 0)
    			{
    				char_decoded[i] = (char) (source[pos] & 0x7F);
    				pos ++;
    			}
    			// 首4位不为1110
        		else if ((source[pos] & 0xE0) != 0xE0) {
        			//l_inner_stream.readByte(b1);
        			char_decoded[i] = (char) (((source[pos] & 0x1F) << 6) | (source[pos+1] & 0x3F));
        			pos += 2;
        		} 
        		else {
        			//l_inner_stream.readByte(b1);
        			//l_inner_stream.readByte(b2);
        			//buffer[i] = (char) (((b[0] & 0x0F) << 12)| ((b1[0] & 0x3F) << 6) | (b2[0] & 0x3F));
        			char_decoded[i] = (char) (((source[pos] & 0x0F) << 12)
        									| ((source[pos+1] & 0x3F) << 6) 
        									| (source[pos+2] & 0x3F));
        			pos += 3;
        		} 
    	}
    	return char_decoded;
    }
    
    
    /*
    public final void WriteLong(long i) throws IOException {
        write4ByteInt((int) (i >> 32));
        write4ByteInt((int) i);
    }
    
    public final long ReadLong() throws IOException {
    	byte[] byte_buff = new byte[8];
        if(l_inner_stream.read(byte_buff)!=-1)
        {
        	return Transform8ByteToLong(byte_buff, 0);
        }
        else
        	return -1;
    	
    } 
    */
    public final void flush() throws IOException
    {
    	l_inner_stream.flush();
    }
	

}

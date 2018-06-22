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
import java.io.UnsupportedEncodingException;

import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.IO.L1.IOStream;
import LCG.StorageEngin.IO.L1.IOStreamNative; 
import LCG.StorageEngin.Serializable.IObject;

public class String256CharsNew extends IObject{

	byte s_byte_length; 
	byte[] content;
	final int  byte_max_length;
	
	public String256CharsNew()
	{
		byte_max_length = 255;
	}
	
	public String256CharsNew(String ss) throws UnsupportedEncodingException
	{  
		byte_max_length = 255;
		content = ss.trim().getBytes("UTF-8");  
		s_byte_length = (byte)Math.min(content.length, byte_max_length);
	}
	
	public void Read(IOInterface io_v) throws IOException
	{
		synchronized(io_v){
			byte[] byte_buff = new byte[1];
			int length;
			if(io_v.read(byte_buff)!=-1)
			{
				length = ((int)byte_buff[0])& 0xFF; 
				s_byte_length = byte_buff[0];
				//content = new char[length]; 
				content = new byte[length];
				//io_v.readChars(content, 0, length);
				io_v.read(content, 0, length);
			}
			else
			{
				content = null;
			} 
		}
	}  

	@Override
	public void Write(IOInterface io_v) throws IOException {
		synchronized(io_v){ 
			int byte_length = Math.min(content.length, byte_max_length); 
			io_v.write1ByteInt(byte_length); 
 
			//io_v.writeChars(content, 0, length);
			io_v.write(content, 0, byte_length);
		}
 
	}
	
	public void Write(IOInterface io_v, String _str) throws IOException {
		synchronized(io_v){ 
			int byte_length = Math.min(_str.getBytes("UTF-8").length, byte_max_length); 
			io_v.write1ByteInt(byte_length); 
 
			//io_v.writeChars(content, 0, length);
			io_v.write(_str.getBytes(), 0, byte_length);
		}
 
	}

	
	public String Get() throws UnsupportedEncodingException
	{
		if(content!=null)
			return new String(content,0,content.length,"UTF-8"); 
		else
			return null;
	}
	
	@Override
	/*
	 * (non-Javadoc)
	 * @see LCG.StorageEngin.Serializable.IObject#Size()
	 * 
	 * returns the byte length
	 */
	public int sizeInBytes() { 
		//return 1 + content.length<<1;//since char has two bytes
		return 1 + content.length;
	}
	
	static int constantSize()
	{
		/*
		 *  it is the 1 byte that records the length of the string.
		 */
		return 1;
	}
 
  

	@Override
	public void setID(int _id) {
		// TODO Auto-generated method stub
		
	}

	 
 
  
	 
}

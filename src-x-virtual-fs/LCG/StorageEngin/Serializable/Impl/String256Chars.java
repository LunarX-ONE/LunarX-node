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
import LCG.StorageEngin.IO.L1.IOStream;
import LCG.StorageEngin.IO.L1.IOStreamNative; 
import LCG.StorageEngin.Serializable.IObject;

public class String256Chars extends IObject{

	byte s_length;
	char[] content;
	
	public String256Chars()
	{
		
	}
	
	public String256Chars(int length, String ss)
	{
		int ll = length;
		if(ll>=256)
		{
			ll =255;
		}
		s_length = (byte) ll;
		
		content = new char[ll];
		ss.getChars(0, ll, content, 0);
	}
	
	public void Read(IOInterface io_v) throws IOException
	{
		synchronized(io_v){
			byte[] byte_buff = new byte[1];
			int length;
			if(io_v.read(byte_buff)!=-1)
			{
				length = ((int)byte_buff[0])& 0xFF; 
				s_length = byte_buff[0];
				content = new char[length]; 
				io_v.readChars(content, 0, length);
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
			int length = content.length;
			if(length >=256 )
				io_v.write1ByteInt(255);
			else
				io_v.write1ByteInt(length); 
 
			io_v.writeChars(content, 0, length);
		}
 
	}

	
	public String Get()
	{
		if(content!=null)
			return new String(content);
		else
			return null;
	}
	
	@Override
	public int sizeInBytes() { 
		return 1 + content.length<<1;//since char has two bytes
	}

 
	 
 

	@Override
	public void setID(int _id) {
		// TODO Auto-generated method stub
		
	}

	 
 
  
	 
}

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
import LCG.StorageEngin.Serializable.IObject;

public class VInt extends IObject{

	int size;
	int value;
	
	public VInt()
	{
		size = 0;
		value = -1;
	}
	
	public VInt(int i)
	{ 
		value = i;  
		while ((i & ~0x7F) != 0) {
			i >>>= 7;
			size++;
        }
		size++;  
	}
	
	public void Read(IOInterface io_v) throws IOException
	{
		size = 0;
		value = -1;
		
		byte[] b = new byte[1];
        if(io_v.read(b)!=-1)
        { 
        	size++;
        	int i = b[0] & 0x7F;
        	for (int shift = 7; (b[0] & 0x80) != 0; shift += 7) {
        		io_v.read(b);
        		i |= (b[0] & 0x7F) << shift;
        		size++;
        	}
        	value = i;
        }
        else
        	value = -1;
	} 
	 
	public int Get()
	{
		return  value;  
	}
	
	
	public void Write(IOInterface io_v) throws IOException {
		while ((value & ~0x7F) != 0) {
			io_v.write((byte) ((value & 0x7f) | 0x80));
			value >>>= 7;
        }
		io_v.write((byte) value);  
	} 
	 
	
	@Override
	public int sizeInBytes() { 
		return size;
	}

	@Override
	public void setID(int _id) {
		// TODO Auto-generated method stub
		
	}

	 

	 
 
	
	
	
}

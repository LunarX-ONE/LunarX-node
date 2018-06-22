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

public class Int4Byte extends IObject{

	int value;
	
	public Int4Byte()
	{
		value = -1;
	}
	
	public Int4Byte(int i)
	{ 
		value = i;  
	}
	
	public void Read(IOInterface io_v) throws IOException
	{
		value = io_v.read4ByteInt();
	} 
	 
	 
	public int Get()
	{
		return value ;  
	}
	
	public void evaluate(int i)
	{
		this.value = i;
	}
	@Override
	public void Write(IOInterface io_v) throws IOException { 
		io_v.write4ByteInt(value); 
	}  
	@Override
	public int sizeInBytes() { 
		return 4;
	}

	@Override
	public void setID(int _id) {
		// TODO Auto-generated method stub
		
	}

	 
 
	
}

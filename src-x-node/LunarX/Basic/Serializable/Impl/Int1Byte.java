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

package LunarX.Basic.Serializable.Impl;

import java.io.IOException;

import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.IO.L1.IOStream;
import LCG.StorageEngin.Serializable.IObject;
import LunarX.Basic.Serializable.Impl.Int1Byte;

public class Int1Byte extends IObject {

	byte value;

	public Int1Byte() {

	}

	public Int1Byte(int i) {
		value = (byte) i;
	}

	public void Read(IOInterface io_v) throws IOException {
		byte[] byte_buff = new byte[1];
		if (io_v.read(byte_buff) != -1) {
			value = byte_buff[0];
		}
	}

	public int Get() {
		return ((int) value) & 0xFF;
	}

	@Override
	public void Write(IOInterface io_v) throws IOException {

		io_v.write(value);

	}

	@Override
	public int sizeInBytes() {
		return 1;
	}

	public static void main(String[] args) throws IOException {
		IOStream read_int, write_int;

		long start = System.currentTimeMillis();

		// 2^12=4*1024=4K buff size
		write_int = new IOStream("D:/NoSQLDB/1ByteInt.pdf", "rw", 25);

		for (int i = 1; i < 256; i++) {
			Int1Byte i1b = new Int1Byte(i);
			i1b.Write(write_int);
		}

		write_int.close();
		System.out.println(
				"IOStream finish writing that Spend: " + (double) (System.currentTimeMillis() - start) / 1000 + "(s)");

		start = System.currentTimeMillis();

		read_int = new IOStream("D:/NoSQLDB/1ByteInt.pdf", "r", 12);

		for (int j = 0; j < 256; j++) {
			Int1Byte s2cs = new Int1Byte();
			s2cs.Read(read_int);
			System.out.println(s2cs.Get());
		}

		read_int.close();
		System.out.println(
				"IOStream finish reading that Spend: " + (double) (System.currentTimeMillis() - start) / 1000 + "(s)");

	}

	@Override
	public void setID(int _id) {
		// TODO Auto-generated method stub

	}
}

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

package Lunarion.SE.AtomicStructure;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.Serializable.IObject;
import LCG.StorageEngin.Serializable.Impl.String256CharsNew;

public class IDStringPair extends IObject {
	public long s_key_id;
	// public byte string_length; //means 256 chars length at most
	public String256CharsNew s_key_string;

	public IDStringPair() {
		s_key_id = -1;
		// s_key_string = null;
		s_key_string = new String256CharsNew();
	}

	public IDStringPair(long k_id, String k_string) throws UnsupportedEncodingException {
		s_key_id = k_id;
		s_key_string = new String256CharsNew(k_string);
	}

	public void Read(IOInterface io_v) throws IOException {
		synchronized (io_v) {
			s_key_id = io_v.ReadLong();
			if (s_key_id != -1)
				s_key_string.Read(io_v);
			else
				s_key_string = null;
		}
	}

	public void Write(IOInterface io_v) throws IOException {
		if (s_key_id != -1 && s_key_string != null) {
			synchronized (io_v) {
				io_v.WriteLong(s_key_id); 
				s_key_string.Write(io_v);// write the key string with length at
											// most
											// 256 chars
			}
		}
	}

	@Override
	public int sizeInBytes() {
		// one long s_key_id and 1 String256Chars
		return 8 + s_key_string.sizeInBytes();
	}

	@Override
	public void setID(int _id) {
		// TODO Auto-generated method stub

	}
}

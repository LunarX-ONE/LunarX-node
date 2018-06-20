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

package Lunarion.SE.HashTable.Stores;

import java.io.IOException;

import LCG.StorageEngin.IO.L1.IOStreamNative;
import Lunarion.SE.AtomicStructure.IDStringPair;

public class StoreIDString extends IOStreamNative {

	public StoreIDString(String file_name, String mode, int bufbitlen) throws IOException {
		super(file_name, mode, bufbitlen);
	}

	 
	public long add(int _id, String string_value) throws IOException {
		synchronized (this) {
			seekEnd();
			long position = length();
			// TODO no new
			IDStringPair kisp = new IDStringPair(_id, string_value);
			kisp.Write(this);
			return position;
		}
	}

	public IDStringPair get(long position) throws IOException {
		synchronized (this) {
			seek(position);
			IDStringPair id_keystring_pair = new IDStringPair();
			id_keystring_pair.Read(this);
			return id_keystring_pair;
		}
	}
}

/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author DADA database team, feiben@lunarion.com
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

package LCG.Utility;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;

import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.Def.DBFSProperties.BlockSizes;

public class VectorUtile {
	
	public static int binarySearch(Object[] a, Object key) {
	    int low = 0;
	    int high = a.length-1;

	    while (low <= high) {
	        int mid = (low + high) >> 1;
	        Comparable midVal = (Comparable)a[mid];
	        int cmp = midVal.compareTo(key);

	        if (cmp < 0)
	        low = mid + 1;
	        else if (cmp > 0)
	        high = mid - 1;
	        else
	        return mid; // key found
	    }
	    return -(low + 1);  // key not found.
	    }

public static void main(String[] args) throws IOException {
		Vector vv = new Vector<Integer>();
		vv.add(222);
		vv.add(333);
        vv.add(444);
        vv.add(555);
		vv.add(100);
		vv.add(101);
		vv.add(200);
		vv.add(210);
		Collections.sort(vv);
        int iii = binarySearch(vv.toArray(),210); 
        System.out.println(iii);
	}
}

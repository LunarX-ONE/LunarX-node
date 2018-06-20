/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contactor: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License.
 * The Original Code is:  LunarBase source code 
 * The LunarBase source code is managed by the development team at Lunarion.com.
 * The Initial Developer of the Original Code is the development team at Lunarion.com.
 * Portions created by lunarion are Copyright (C) lunarion.
 * All Rights Reserved.
 *******************************************************************************
 * 
 */
 

package LunarX.Node.Utils;

import LunarX.Node.Utils.*;

public class HashCadidates {
	 
	public HashCadidates(int default_capacity) {
		 
	}
 
	public static int jvmDefault(String str )
	{
		return Math.abs(str.hashCode()); 
	}
	
	public static long hashJVM(String str )
	{
		return  Math.abs(GeneralHashFunctions.BKDRHash( str));
		//return  Math.abs(GeneralHashFunctions.DEKHash( str));
		//return Math.abs(str.hashCode()); 
	}
	
	 
	
}

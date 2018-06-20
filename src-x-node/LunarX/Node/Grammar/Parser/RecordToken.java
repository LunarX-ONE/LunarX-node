
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
package LunarX.Node.Grammar.Parser;

public class RecordToken {
	 
		static final char _begin = '{';
		static final char _end = '}';
		static final char _evaluate = '=';
		
		/*
		 * text column begins with [\", and ends with ]\"
		 */
		static final String _begin_text = "=[\"";
		static final String _end_text = "\"]"; 
		static final char _possible_begin_text = '['; 
		static final char _possible_end_text = '\"'; 
		static final char _comma = ',';
		static final char _nothing = ' ';
 
	
}

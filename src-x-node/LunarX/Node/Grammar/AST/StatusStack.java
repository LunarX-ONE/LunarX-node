
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
package LunarX.Node.Grammar.AST;

import java.util.Vector;

import LunarX.Node.Grammar.Parser.ParserStatus;

 
public class StatusStack {
	Vector<ParserStatus> stack = new Vector<ParserStatus>();
	
	public StatusStack()
	{
		
	}
	
	public ParserStatus currentStatus()
	{
		return stack.elementAt(stack.size()-1);
	}
	
	public boolean removeTop()
	{
		if(stack.size()<=0)
			return false;
		
		stack.remove(stack.size()-1);
		return true;
	}
	
	public void addTop(ParserStatus ps)
	{
		stack.add(ps);
	}
}

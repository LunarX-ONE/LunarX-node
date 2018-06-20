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


package LunarX.Node.Grammar.AST.Expression; 

public abstract class BinaryLogicalExpression extends Expression {
	
	private Expression left_expr;
	private Expression right_expr;
	 
	public BinaryLogicalExpression( Expression _left, Expression _right)
	{
		 
		this.left_expr = _left;
		this.right_expr = _right; 
		
	}
	
	public Expression left()
	{
		return this.left_expr;
	}
	
	public Expression right()
	{
		return this.right_expr;
	}
 

}

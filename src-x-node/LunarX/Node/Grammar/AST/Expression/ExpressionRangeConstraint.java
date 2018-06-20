
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

import LCG.EnginEvent.Interfaces.LFuture;
import LunarX.Node.Grammar.AST.Visitor.AbstractVisitor;

public class ExpressionRangeConstraint extends Expression{

	final public String _table;  
	final public String _column;
	final public int _key_start;
	final public int _key_end;
	 
	
	public ExpressionRangeConstraint(String table, String col, int key_start, int key_end) {
		this._table = table;
		_column = col;
		_key_start = key_start;
		_key_end = key_end; 
	} 
	 
	public String toString()
	{
		return _table + ":" + _key_start + "<" + _column + "<" + _key_end;
	}

	@Override
	public <T> LFuture<T> accept(AbstractVisitor visitor) {
		return new LFuture(visitor.visitRangeConstraint(this)); 
	} 

}

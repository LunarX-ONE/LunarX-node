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

import LCG.EnginEvent.Event;
import LCG.EnginEvent.Interfaces.LFuture;
import LunarX.Node.Grammar.AST.Visitor.AbstractVisitor;

public abstract class Expression {
	
	/*
	 * all the expressions and their derivative sons has 
	 * no knowledge of the return type.
	 * 
	 * Hence use LFuture to accept it. The visitor decides the type 
	 * and the caller claims the type:
	 * 
	 * LFuture<int[][]> result = Expression.accept(visitor);
	 * int[][] r = result.get();
	 * 
	 * The same to LCG.EnginEvent.EventDispatcher.dispatch(Event content)
	 */
	public abstract <T> LFuture<T> accept(AbstractVisitor visitor);

}

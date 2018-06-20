
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
package LunarX.Node.Grammar.AST.Visitor;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

import LunarX.Node.EDF.Events.QuerySimple;
import LunarX.Node.Grammar.AST.Expression.BinaryLogicalExpression;
import LunarX.Node.Grammar.AST.Expression.ExpressionConstant;
import LunarX.Node.Grammar.AST.Expression.ExpressionKeywords;
import LunarX.Node.Grammar.AST.Expression.ExpressionPointConstraint;
import LunarX.Node.Grammar.AST.Expression.ExpressionRangeConstraint;

public interface AbstractVisitor {

	public HashMap<Integer, Integer> visitAND(BinaryLogicalExpression expr);
	public HashMap<Integer, Integer> visitOR(BinaryLogicalExpression expr);
	public HashMap<Integer, Integer> visitRangeConstraint(ExpressionRangeConstraint expr) ;
	public HashMap<Integer, Integer> visitPointConstraint(ExpressionPointConstraint expr) ;
	public HashMap<Integer, Integer> visitValue(ExpressionConstant expr) ;
	public HashMap<Integer, Integer> visitKeywords(ExpressionKeywords expr, int from, int count ) ;
	
	public void shutDown();
}

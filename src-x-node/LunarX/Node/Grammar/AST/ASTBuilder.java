
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

import LunarX.Node.Grammar.AST.Expression.Expression;
import LunarX.Node.Grammar.Parser.LexerSubExpression;
import LunarX.Node.Grammar.Parser.ParserStatus;
import LunarX.Node.Grammar.Parser.QueryToken;

public class ASTBuilder {

	/*
	 * build AST for WHERE clause for one table:
	 * "column_1 = value 
	 *  and (column_2 >= 100 and column_3 <10) 
	 *  or column_4 against (\" keyword1 + keyword2 \")
	 *  and column_5 against (\" keyword1 keyword2 \")"
	 *  
	 *  Plus(+) stands for AND, while space is OR. 
	 *  
	 *  In mysql, the fulltext query grammar is:
	 *  match(column1,column2,...) against (\" keyword1 + keyword2  \");
	 *  
	 *  Here Lunarbase is a little different.
	 *  
	 */
	
	StatusStack status_stack = new StatusStack();
	 public ASTBuilder()
	 {
		 
	 }
	
	 /*
	  * returns the root of the expression tree.
	  */
	 public Expression build(String query)
	 {
		 return null;
	 }
	 
	 public Expression buildExpression(String subquery, int start_at, QueryToken end_token)
	 {
		 return null;
	 }
	
}

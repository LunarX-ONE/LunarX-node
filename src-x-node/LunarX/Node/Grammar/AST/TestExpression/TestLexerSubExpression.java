
/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contacts: 
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
package LunarX.Node.Grammar.AST.TestExpression;

import LunarX.Node.Grammar.AST.Expression.Expression;
import LunarX.Node.Grammar.Parser.LexerSubExpression;

public class TestLexerSubExpression {
	
	
	public static void  testNextSubExpression() throws Exception
	{
		String query1 = "100 <    col  ";
		String query2 = "100 <    col< 300  ";
		String query3 = "col<=  300  ";
		String query4 = "100 < col<=  300  ";
		String query5 = "     100 >   = col >  300  ";
		String query6 = " col >=  300  ";
		
		/*
		 * should be wrong statement 
		 */
		String query7 = " 600 > col >=  300 >200  ";
		/*
		 * maybe right, 600 > col >= 500
		 */
		String query8 = " 600 > 300 > col >= 500  ";
		/*
		 * maybe right, 600 >= col
		 */
		String query9 = " 600 > 300 > 500>= col   ";
		
		LexerSubExpression l = new LexerSubExpression("table", query1, true);
		Expression e = l.nextSubExpression(0); 
		System.out.println(e.toString());
		
		l = new LexerSubExpression("table", query2, true);
		e = l.nextSubExpression(0); 
		System.out.println(e.toString());
		
		l = new LexerSubExpression("table", query3, true);
		e = l.nextSubExpression(0); 
		System.out.println(e.toString());
		
		l = new LexerSubExpression("table", query4, true);
		e = l.nextSubExpression(0); 
		System.out.println(e.toString());
		
		l = new LexerSubExpression("table", query5, true);
		e = l.nextSubExpression(0); 
		System.out.println(e.toString());
		
		l = new LexerSubExpression("table", query6, true);
		e = l.nextSubExpression(0); 
		System.out.println(e.toString());
		
		l = new LexerSubExpression("table", query9, true);
		e = l.nextSubExpression(0); 
		System.out.println(e.toString());
	}
	
	public static void testKeywordExpression() throws Exception
	{
		String query1 = " col against (\" keyword1, keyowrd2,\")  ";
		LexerSubExpression l = new LexerSubExpression("table", query1, true);
		Expression e = l.nextSubExpression(0); 
		System.out.println(e.toString());
		
		String query2 = "col against (\" ,keyword1, keyowrd2, + keyword3\")  ";
		l = new LexerSubExpression("table", query2, true);
		e = l.nextSubExpression(0); 
		System.out.println(e.toString());
		
		String query3 = "col against (\" keyword1 ,  \")  ";
		l = new LexerSubExpression("table", query3, true);
		e = l.nextSubExpression(0); 
		System.out.println(e.toString());
	}
	
	
	/*
	 * test "expr_0 AND expr_1 OR expr_2 ......
	 */
	public static void testConbinedExpression() throws Exception
	{
		String query1 = " col1 < 100 and 2000 > col2 > 1000 ";
		LexerSubExpression l = new LexerSubExpression("table", query1, true);
		Expression e = l.build(0); 
		if(e!=null)
			System.out.println(e.toString()); 
		
		String query2 = " col1 < 100 or 2000 > col2 > 1000 ";
		l = new LexerSubExpression("table", query2, true);
		e = l.build(0); 
		if(e!=null)
			System.out.println(e.toString()); 
		
		String query3 = " 0 < col1 < 100 and col3 >= 100    or 2000> col2 >1000 ";
		l = new LexerSubExpression("table", query3, true);
		e = l.build(0); 
		if(e!=null)
			System.out.println(e.toString()); 
		 
	}
	
	public static void main(String[] args) throws Exception {
		 
		//testNextSubExpression() ;
		//testKeywordExpression() ;
		testConbinedExpression();
	}

}

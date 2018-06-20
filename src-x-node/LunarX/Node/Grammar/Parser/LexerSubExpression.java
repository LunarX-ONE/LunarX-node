
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

import LunarX.Node.EDF.Events.IncommingRecords;
import LunarX.Node.Grammar.AST.StatusStack;
import LunarX.Node.Grammar.AST.Expression.BinaryLogicalExpression;
import LunarX.Node.Grammar.AST.Expression.Expression;
import LunarX.Node.Grammar.AST.Expression.ExpressionAND;
import LunarX.Node.Grammar.AST.Expression.ExpressionConstant;
import LunarX.Node.Grammar.AST.Expression.ExpressionKeywords;
import LunarX.Node.Grammar.AST.Expression.ExpressionOR;
import LunarX.Node.Grammar.AST.Expression.ExpressionPointConstraint;
import LunarX.Node.Grammar.AST.Expression.ExpressionRangeConstraint;
import LunarX.RecordTable.StoreUtile.LunarColumn;

public class LexerSubExpression {
	
	private final String query;
	private final String table;
	private int current_pos;
	 
	private QueryToken current_token;
	 
	StatusStack status_stack;
	private char char_at;
	
	public LexerSubExpression(String _table, String input_query, boolean skip_comment)
	{
		this.table = _table;
		this.query = input_query;
		this.current_pos = 0;
		status_stack = new StatusStack();
		 
		scanChar();    
	}
	
	private void scanChar()
	{
		char_at = query.charAt(current_pos);
		current_pos++;
	}
	
	public char currentChar()
	{
		return char_at;
	}
	
	public QueryToken currentToken()
	{
		return this.current_token;
	}
	
	private String readValue()
	{
		 
		int start = current_pos;
		/*
		 * skip the blanks
		 */
		while(query.charAt(current_pos) ==  QueryToken.END_EXPR.getChar())
			current_pos++;
		
		while(query.charAt(current_pos) != QueryToken.END_EXPR.getChar() 
				&&query.charAt(current_pos) != QueryToken.END_SUB_EXPR.getChar()
				&&!this.isEnd())
		{
			current_pos++;
		}
		current_pos++;
		
		return query.substring(start, current_pos-1);
		
	}
	
	private ExpressionConstant readValueExpression()
	{
		 
		int start = current_pos;
		/*
		 * skip the blanks
		 */
		while(query.charAt(current_pos) ==  QueryToken.END_EXPR.getChar())
			current_pos++;
		
		start = current_pos; 
		 
		while(!QueryToken.stop_chars.contains(query.charAt(current_pos)))
		{
			current_pos++;
		}
	 
		
		return new ExpressionConstant(query.substring(start, current_pos));
		
	}
	
	/*
	 * subexpression includes these things:
	 * 100 < column 
	 * 100 <= column < 200 
	 * column > 100 
	 * 200 >= column > 100
	 * column = 100 
	 * column against(" good, cheap") 
	 * 
	 * for ranges and points, it supports integer only.
	 * 
	 * after extraction, the global current_pos points to the end of the 
	 * expression + 1, which may exceed the string boundary.
	 */
	public Expression nextSubExpression(int __start_at ) throws Exception 
	{
		Expression ep = null;
	 
		if(isEnd())
			return null;
		
		current_pos = __start_at; 
		
		ExpressionConstant left = readValueExpression(); 
		/*
		 * skip the blanks
		 */
		while(query.charAt(current_pos) ==  QueryToken.BLANK.getChar())
		{
			current_pos++;
			if(isEnd())
				return left;
		}
		
		QueryToken token;
		char ch = query.charAt( current_pos);
		switch(ch)
		{
			case '=':
				current_pos++;
				token = QueryToken.EQUAL;
				 
				break;
			case '<':
				int less_token_end_at = PatternInterpreter.confirmToken(
									QueryToken.LESS_EQUAL.getString(), 
									query, 
									current_pos);
				/*
				 * it is a LESS_THAN token: <
				 */
				if(less_token_end_at == -1) 
				{	
					token = QueryToken.LESS_THAN; 
					current_pos ++;
				}
				/*
				 * else it is a LESS_EQUAL token: <
				 */
				else
				{	
					token = QueryToken.LESS_EQUAL;
					current_pos = less_token_end_at+1;
				}
			 
				break;
			case '>':
				int great_token_end_at= PatternInterpreter.confirmToken(
						QueryToken.GREATE_EQUAL.getString(), 
						query, 
						current_pos);
				if(great_token_end_at == -1) 
				{	
					token = QueryToken.GREATE_THAN; 
					current_pos++;
				}
				/*
				 * else it is a LESS_EQUAL token: <
				 */
				else
				{	
					token = QueryToken.GREATE_EQUAL;
					current_pos = great_token_end_at+1;
				} 
				break;
			case 'a':
				int against_token_end_at = PatternInterpreter.confirmToken(
									QueryToken.AGAINST.getString(), 
									query, 
									current_pos);
				/*
				 * it is not an against token: against
				 * then just return the left expression.
				 */
				if(against_token_end_at == -1) 
				{	
					token =  null;
					
					return left; 
					//throw new Exception("[ERROR]: @nextSubExpression illegal statment "
					//		+ " of the \"against\" token. please check your query."); 
				}
				/*
				 * else it is an against token: against, 
				 * then returns the ExpressionKeywords. 
				 * this sub-expression finishes.
				 */
				else
				{	
					token = QueryToken.AGAINST;
					current_pos = against_token_end_at+1;
					ExpressionKeywords e_k = againstExpression(current_pos); 
					if(e_k == null)
						return null;
					
					e_k.setTable(this.table);
					e_k.setColumn(left._value);
					return e_k;
					
				} 
			default:
				/*
				 * it is just an ExpressionConstant
				 */
				return left; 
		}
		 
		Expression right = nextSubExpression(current_pos);
		if(right == null)
			return left;
		
		String col = "";
		String val = "";
		/*
		 * left is the column, then the right must be a value
		 */
		if(!PatternInterpreter.confirmNumber(left._value))
		{			 
			col = left._value;
			if(right.getClass() == ExpressionConstant.class)
			{ 
				val = ((ExpressionConstant)right)._value;
			}  
			else
			{ 
				throw new Exception("[ERROR]: @LexerSubExpression.nextSubExpression illegal statement "
						+ " of the right expression, which has to be a value. please check your query."); 
			}  
		}
		/*
		 * left is a number, then right must be a column 
		 * or an expression of inequality. 
		 * otherwise an error.
		 */
		else
		{ 
			if(right.getClass() == ExpressionConstant.class)
			{
				col = ((ExpressionConstant)right)._value;
				val = left._value;
				
				/*
				 * if the right is still a number, then the express is wrong.
				 */
				if( PatternInterpreter.confirmNumber(col))
					throw new Exception("[ERROR]: @LexerSubExpression.nextSubExpression illegal statement "
							+ " of the right expression, which has to be a column. please check your query."); 
			 
				token = QueryToken.reverseToken(token);
				
			}
			/*
			 * else if right is an inequality expression:
			 * 100 < column <= 200
			 * the right is an expression of 
			 * Interger.minimumValue < column <= 200
			 * 
			 * just replace the integer minimumValue to be the left value 100;
			 */
			else
			{
				col =  ((ExpressionRangeConstraint)right)._column ;
				int lower = ((ExpressionRangeConstraint)right)._key_start;
				int upper = ((ExpressionRangeConstraint)right)._key_end; 
				 
				switch(token)
				{
					case LESS_THAN:
					case LESS_EQUAL:
						/*
						 * replace the lower bound of right expression 
						 */
						ep = new ExpressionRangeConstraint(table, 
								col,
								Integer.parseInt(left._value),
								upper);
						break;
					case GREATE_THAN:
					case GREATE_EQUAL:
						/*
						 * replace the upper bound of right expression 
						 */
						ep = new ExpressionRangeConstraint(table, 
								col,
								lower,
								Integer.parseInt(left._value));
						break;
					default:
						/*
						 * otherwise, ep is a wrong expression
						 */
						ep = null;
						break;
				}
				return ep;
			} 
		}
		
		switch(token)
		{
			case LESS_THAN:
			case LESS_EQUAL:
				ep = new ExpressionRangeConstraint(table, 
						col,
						Integer.MIN_VALUE,
						Integer.parseInt(val));
				break;
			case GREATE_THAN:
			case GREATE_EQUAL:
				ep = new ExpressionRangeConstraint(table, 
						col,
						Integer.parseInt(val),
						Integer.MAX_VALUE );
				break;
			case EQUAL:
				ep = new ExpressionPointConstraint(table,  
						col,
						val);
				break; 
			default:
				ep = null;
				break;
		}  
		return ep;
	}
	
	
	private boolean endKeywords(int __current_pos)
	{
		return ((query.charAt( __current_pos) == '\"')
				&& PatternInterpreter.confirmToken(
						QueryToken.END_KEYWORD.getString(), 
						query, 
						__current_pos) != -1);
	}
	/* 
	 * column against(" good, cheap")  
	 * start must at (  
	 */
	public ExpressionKeywords againstExpression(int __start_at )  
	{
		ExpressionKeywords e_k = null;
		current_pos = __start_at;
		/*
		 * skip the blanks
		 */
		while(query.charAt(current_pos) ==  QueryToken.BLANK.getChar())
		{ 
			current_pos++;
			if(isEnd())
				return e_k;
		}
		char ch = query.charAt( current_pos);
		switch(ch)
		{
			case '(':
				int _token_end_at = PatternInterpreter.confirmToken(
						QueryToken.BEGIN_KEYWORD.getString(), 
						query, 
						current_pos);
				 
				if(_token_end_at == -1) 
				{	 
					return e_k;  
				}
				/*
				 * else it is ("
				 */
				else
				{	
					current_pos = _token_end_at +1;				
					 
					String keyword = nextKeyword(current_pos);	 
					if(keyword == null)
						return null;
					e_k = new ExpressionKeywords(keyword);
					
					while(!endKeywords(current_pos))
					{
						QueryToken current_op = nextKeywordOps(current_pos);
						 
						String keyword_i = nextKeyword(current_pos);	 
						 
						if(keyword_i != null )
							e_k.addKeyword(current_op, keyword_i);
					} 
				}
				break;
			default:
				return e_k;
		}
		
		return e_k;
				
	}
	
	/* 
	 * build AST for WHERE clause for one table:
	 * "column_1 = value 
	 *  and (column_2 >= 100 and column_3 <10) 
	 *  or column_4 against (\" keyword1 + keyword2 \")
	 *  and column_5 against (\" keyword1 keyword2 \")"
	 *  
	 *  Plus(+) stands for AND, while space and comma(,) is OR. 
	 *  
	 *  In mysql, the fulltext query grammar is:
	 *  match(column1,column2,...) against (\" keyword1 + keyword2  \");
	 *  
	 *  Here Lunarbase is a little different.
	 *  
	 */
	/*
	 * discard incomplete sub-expressions.  
	 */
	public Expression build(int start_at )  
	{
		Expression ep = null;
		 
		current_pos = start_at; 
		if(isEnd())
			return null;
		
		Expression left_expr = null;
		try
		{
			this.nextSubExpression(current_pos); 
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
		
		/*
		 * if left_expr is just a constant, return null;
		 */
		if(left_expr.getClass() == ExpressionConstant.class)
		{
			return null;
		}
		if(isEnd())
			return left_expr;
		/*
		 * skip the blanks
		 */
		while(query.charAt(current_pos) ==  QueryToken.BLANK.getChar())
		{
			current_pos++;
			if(isEnd())
				return left_expr;
		}
		
		QueryToken token;
		char ch = query.charAt( current_pos);
		switch(ch)
		{ 
			case 'a':
				int and_token_end_at = PatternInterpreter.confirmToken(
									QueryToken.AND.getString(), 
									query, 
									current_pos);
				/*
				 * it is not an AND token, then it is unknown, 
				 * then discard the remaining expression, 
				 * returns the left sub-expression.
				 */
				if(and_token_end_at == -1) 
				{	
					 
					current_pos ++;
					return left_expr; 
				}
				/*
				 * else it is an AND token 
				 */
				else
				{	
					token = QueryToken.AND;
					current_pos = and_token_end_at+1;
				}
			 
				break;
			case 'o':
				int or_token_end_at= PatternInterpreter.confirmToken(
						QueryToken.OR.getString(), 
						query, 
						current_pos);
				if(or_token_end_at == -1) 
				{  
					/*
					 * it is unknown, then discard the remaining expression, 
					 * returns the left sub-expression.
					 */
					current_pos++;
					return left_expr; 
				}
				/*
				 * else it is an OR token 
				 */
				else
				{	
					token = QueryToken.OR;
					current_pos = or_token_end_at+1;
				} 
				break;
			 
			default:
				/*
				 * it is unknown, then discard the remaining expression, 
				 * returns the left sub-expression.
				 */
				return left_expr; 
		}
		 
		//Expression right_expr = nextSubExpression(current_pos);
		Expression right_expr = build(current_pos);
		
		if(right_expr == null || right_expr.getClass() == ExpressionConstant.class)
		{
			return left_expr;
		}
		
		switch(token)
		{
			case AND: 
				ep = new ExpressionAND(left_expr, right_expr);
				break;
			case OR:
				ep = new ExpressionOR(left_expr, right_expr);
				break;  
			default:
				ep = null;
				break;
		}  
		return ep; 
	}
	
	private String nextKeyword(int __start_at)
	{
		current_pos = __start_at;
		int key_start = __start_at;
		if(isEnd())
			return null;
		/*
		 * skip the blanks
		 */
		while(query.charAt(current_pos) ==  QueryToken.BLANK.getChar())
		{ 
			current_pos++;
			if(isEnd())
				return null;
		}
		key_start = current_pos;
		
		while(!QueryToken.keyword_ops.contains(query.charAt( current_pos))
				&& !endKeywords(current_pos)) 
		{
			current_pos++;
			if(isEnd())	
			{
				String keyword = query.substring(key_start,current_pos ); 
				return keyword;
			}
		}
		if(key_start == current_pos)
			return null;
		String keyword = query.substring(key_start, current_pos);
		 
		return keyword;
	}
	
	private QueryToken nextKeywordOps(int __start_at)
	{
		current_pos = __start_at;
		if(isEnd())
			return null;
		/*
		 * if a plus(+) appears, the token is AND_KEYWORD
		 */
		boolean and_appears = false;
		
		while(QueryToken.keyword_ops.contains(query.charAt( current_pos)) ) 
		{
			if(query.charAt( current_pos) == QueryToken.AND_KEYWORD.getChar())
				and_appears = true;
			current_pos++;
		}
		if(and_appears)
			return QueryToken.AND_KEYWORD;
		else
			return QueryToken.OR_KEYWORD;
		
	}
	
	public boolean isEnd() {
		return current_pos >= query.length();
	}
	

	
}

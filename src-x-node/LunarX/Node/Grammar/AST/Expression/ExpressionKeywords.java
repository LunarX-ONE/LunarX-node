
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

import java.util.HashMap;
import java.util.Vector;

import LCG.EnginEvent.Interfaces.LFuture;
import LunarX.Node.API.LunarTable;
import LunarX.Node.Grammar.AST.Visitor.AbstractVisitor;
import LunarX.Node.Grammar.Parser.QueryToken;
import LunarX.Node.SetUtile.SetOperation;

public class ExpressionKeywords extends Expression{

	String table;
	String column;
	int from;
	int count;
	
	private Vector<String> keywords = new Vector<String>(); 
	private Vector<QueryToken> operators = new Vector<QueryToken>();
	private SetOperation s_o = new SetOperation(); 
	 
	public ExpressionKeywords( String keyword ) { 
		keywords.add(keyword); 
		operators.add(null);
		from = 0;
		count = 0;
	} 
	
	public void addKeyword(QueryToken _operation, String keyword)
	{
		keywords.add(keyword);
		if(QueryToken.keyword_ops.contains(_operation.getChar() ))
			operators.add(_operation);
		else
			operators.add(QueryToken.OR_KEYWORD);
	} 

	public Vector<String> getKeywords()
	{
		return this.keywords;
	}
	public Vector<QueryToken> getOps()
	{
		return this.operators;
	}
	
	public void setTable(String _table)
	{
		this.table = _table;
	}
	public void setColumn(String _col)
	{
		this.column = _col;
	}
	public String getColumn()
	{
		return this.column;
	}
	public void setCount(int __from, int __latest_count)
	{
		this.from = __from;
		this.count = __latest_count;
	}
	
	public String toString()
	{
		int indicator_first = keywords.size()-2;
		int indicator_second = keywords.size()-1;
		String cadidate_2 = keywords.elementAt(indicator_second);
		while(indicator_first>=0)
		{
			String cadidate_1 = keywords.elementAt(indicator_first);
			if(cadidate_1 != null)
			{
				QueryToken ops = operators.elementAt(indicator_second);
				switch(ops)
				{
				case AND_KEYWORD:
					cadidate_2 = cadidate_1 + " + " + cadidate_2;
					break;
				case OR_KEYWORD:
					cadidate_2 = cadidate_1 + " , " + cadidate_2;
					break;
				default:
					break;
				}
			} 
			
			indicator_first--;
			indicator_second--;
		}
		
		return cadidate_2;
	}
	
	/*
	 * <id, score>
	 */
	/*
	public HashMap<Integer, Integer> execute(LunarTable l_table )
	{
		int indicator_first = keywords.size()-2;
		int indicator_second = keywords.size()-1;
		String cadidate_2 = keywords.elementAt(indicator_second);
		int[] result2 = l_table.queryFullText(this.column, cadidate_2, count);
		HashMap<Integer, Integer> result =  s_o.union(result2, new HashMap<Integer,Integer>()) ;
		 
		while(indicator_first>=0)
		{
			String cadidate_1 = keywords.elementAt(indicator_first);
			if(cadidate_1 != null)
			{
				int[] result1 = l_table.queryFullText(this.column, cadidate_1, count);
				
				QueryToken ops = operators.elementAt(indicator_second);
				switch(ops)
				{
				case AND_KEYWORD:
					result = s_o.intersect(result1, result ) ;
					break;
				case OR_KEYWORD:
					result = s_o.union(result1, result ) ;
					break;
				default:
					result = s_o.union(result1, result ) ;
					break;
				}
			} 
			
			indicator_first--;
			indicator_second--;
		}
		
		return result ;
	}
	
	*/
	@Override
	public <T> LFuture<T> accept(AbstractVisitor visitor) {
		return new LFuture(visitor.visitKeywords(this, this.from, this.count)); 
	} 

}

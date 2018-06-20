
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
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import LCG.EnginEvent.Interfaces.LFuture;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.API.LunarTable;
import LunarX.Node.Grammar.AST.Expression.BinaryLogicalExpression;
import LunarX.Node.Grammar.AST.Expression.ExpressionConstant;
import LunarX.Node.Grammar.AST.Expression.ExpressionKeywords;
import LunarX.Node.Grammar.AST.Expression.ExpressionPointConstraint;
import LunarX.Node.Grammar.AST.Expression.ExpressionRangeConstraint;
import LunarX.Node.Grammar.Parser.QueryToken;
import LunarX.Node.SetUtile.SetOperation;

public class BinaryLogicalVisitor implements AbstractVisitor  {

	private final LunarTable table_instance;
	private final SetOperation set_operation;
	private ExecutorService reader_thread_executor;
	
	/*
	 * since it is a binary logical visitor, always 
	 * use an extra tread to calculate union and intersection.
	 */
	final int tread_for_visitor = 1;
	
	public BinaryLogicalVisitor(LunarTable _table, ExecutorService _reader_thread_executor )
	{
		this.table_instance = _table;
		this.set_operation = new SetOperation();
		
		//reader_thread_executor = this.reader_thread_executor = Executors.newFixedThreadPool(tread_for_visitor);
		reader_thread_executor = _reader_thread_executor;
	}

	@Override
	public HashMap<Integer, Integer>  visitAND(BinaryLogicalExpression expr) {
		LFuture<HashMap<Integer, Integer> > left_ids = expr.left().accept(this);		 
		LFuture<HashMap<Integer, Integer> > right_ids = expr.right().accept(this);
		
		 
		return set_operation.intersectSets(left_ids.get(), right_ids.get());
	}

	@Override
	public HashMap<Integer, Integer> visitOR(BinaryLogicalExpression expr) {
		LFuture<HashMap<Integer, Integer>> left_id_scores = expr.left().accept(this);		 
		LFuture<HashMap<Integer, Integer>> right_id_scores = expr.right().accept(this);
		
		return set_operation.union(left_id_scores.get(),right_id_scores.get()); 
		
	}

	@Override
	public HashMap<Integer, Integer> visitRangeConstraint(ExpressionRangeConstraint expr) {
		if (expr.getClass() == ExpressionRangeConstraint.class) {
	    	 
			int[] result = table_instance.queryRangeIDs( 
										expr._column, 
										expr._key_start,
										expr._key_end);  
			if(result != null)
			{
				HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
				
				for(int i=0;i<result.length;i++)
					map.put(result[i], 0);
				
				return map;
			}
			else
				return null;
			
	    }
	    return null;
	}

	 

	@Override
	/*
	 * <id, score>
	 */
	public HashMap<Integer, Integer> visitKeywords(ExpressionKeywords expr, int from, int count) {
		
		Vector<String> keywords = expr.getKeywords();
		Vector<QueryToken> operators = expr.getOps();
		String column = expr.getColumn();
		int indicator_first = keywords.size()-2;
		int indicator_second = keywords.size()-1;
		String cadidate_2 = keywords.elementAt(indicator_second);
		
		
		int[] result2 = null;
		QueryToken last_ops = operators.elementAt(indicator_second);
		if(last_ops == QueryToken.OR_KEYWORD)
			result2 = table_instance.queryFullText(column, cadidate_2, from+count, reader_thread_executor);
		else 
			result2 = table_instance.queryFullText(column, cadidate_2, 0, reader_thread_executor);
		 
		HashMap<Integer, Integer> result =  set_operation.union(result2, new HashMap<Integer,Integer>()) ;
		 
		while(indicator_first>=0)
		{
			String cadidate_1 = keywords.elementAt(indicator_first);
			if(cadidate_1 != null)
			{
				int[] result1 = null;
				QueryToken ops = operators.elementAt(indicator_second);
				switch(ops)
				{
				case AND_KEYWORD:
					result1 = table_instance.queryFullText(column, cadidate_1, 0, reader_thread_executor);
					
					result = set_operation.intersect(result1, result ) ;
					break;
				case OR_KEYWORD:
					result1 = table_instance.queryFullText(column, cadidate_1, from+count, reader_thread_executor);
					
					result = set_operation.union(result1, result ) ;
					break;
				default:
					table_instance.queryFullText(column, cadidate_1, from+count, reader_thread_executor);
					
					result = set_operation.union(result1, result ) ;
					break;
				}
			} 
			
			indicator_first--;
			indicator_second--;
		}
		
		return result ;
	}

	@Override
	public HashMap<Integer, Integer> visitPointConstraint(ExpressionPointConstraint expr) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashMap<Integer, Integer> visitValue(ExpressionConstant expr) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void shutDown() {
		//if(reader_thread_executor != null)
		//	reader_thread_executor.shutdown();
		
	}
	 
	

}

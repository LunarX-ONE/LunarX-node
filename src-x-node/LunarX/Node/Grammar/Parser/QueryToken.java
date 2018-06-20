
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

import java.util.HashSet;
import java.util.Set;

public enum QueryToken { 
	
	    AND {
			public String getString(){return "and";} 
			public char getChar(){return '\0';} 
		}, 
	    OR{
			public String getString(){return "or";} 
			public char getChar(){return '\0';} 
		},
	    EQUAL{
			public String getString(){return "=";} 
			public char getChar(){return '=';} 
		}, 
	    END_EXPR{
			public String getString(){return " ";} 
			public char getChar(){return ' ';} 
		}, 
	    BLANK{
			public String getString(){return " ";} 
			public char getChar(){return ' ';} 
		}, 
	    LESS_THAN {
			public String getString(){return "<";} 
			public final char getChar(){return '<';} 
		}, 
	    LESS_EQUAL("<="){
			public String getString(){return "<=";} 
			public char getChar(){return '\0';} 
		}, 
	    GREATE_THAN(">"){
			public String getString(){return ">";} 
			public char getChar(){return '>';} 
		}, 
	    GREATE_EQUAL(">="){
			public String getString(){return ">=";} 
			public char getChar(){return '\0';} 
		}, 
	    AGAINST {
			public String getString(){return "against";} 
			public char getChar(){return '\0';} 
		}, 
	    BEGIN_KEYWORD("(\""){
			public String getString(){return "(\"";} 
			public char getChar(){return '\0';} 
		}, 
	    END_KEYWORD("\")"){
			public String getString(){return "\")";} 
			public char getChar(){return '\0';} 
		}, 
	    AND_KEYWORD("+"){
			public String getString(){return "+";} 
			public char getChar(){return '+';} 
		}, 
		OR_KEYWORD(" "){
			public String getString(){return " ";} 
			public char getChar(){return ' ';} 
		},  
		OR2_KEYWORD(","){
			public String getString(){return ",";} 
			public char getChar(){return ',';} 
		},  
		BEGIN_SUB_EXPR("("){
			public String getString(){return "(";} 
			public char getChar(){return '(';} 
		}, 
		END_SUB_EXPR(")"){
			public String getString(){return ")";} 
			public char getChar(){return ')';} 
		} ;  
	public abstract String getString();
	public abstract char getChar(); 
 
	/*
	public static final String AND = "and"; 
	public static final String OR = "or";  
	public static final char EQUAL = '='; 
	public static final char END_EXPR = ' ';
	public static final char LESS_THAN = '<';
	public static final String LESS_EQUAL = "<=";
	public static final char GREATE_THAN = '>';
	public static final String GREATE_EQUAL = ">=";
    
	public static final String AGAINST = "against";
	public static final String BEGIN_KEYWORD = "(\"";
	public static final String END_KEYWORD = "\")";
	public static final char AND_KEYWORD = '+';
	public static final char OR_KEYWORD =  ' ';
	
	public static final char BEGIN_SUB_EXPR = '(';
	public static final char END_SUB_EXPR = ')';
	*/
	
	public final String name;
	
	public static final Set<Character> stop_chars = new HashSet<Character>();
	static { 
		stop_chars.add(EQUAL.getChar());
		stop_chars.add(END_EXPR.getChar());
		stop_chars.add(BLANK.getChar());
		stop_chars.add(LESS_THAN.getChar());
		stop_chars.add(GREATE_THAN.getChar());
		stop_chars.add(BEGIN_KEYWORD.getChar());
		stop_chars.add(END_KEYWORD.getChar());
		stop_chars.add(AND_KEYWORD.getChar());
		stop_chars.add(BEGIN_SUB_EXPR.getChar());
		stop_chars.add(END_SUB_EXPR.getChar()); 
	}
	
	public static final Set<Character> keyword_ops = new HashSet<Character>();
	static { 
		keyword_ops.add(AND_KEYWORD.getChar() );
		keyword_ops.add(OR_KEYWORD.getChar() ); 
		keyword_ops.add(OR2_KEYWORD.getChar() ); 
	}
	
	QueryToken(){
        this(null);
    }

	QueryToken(String name){
        this.name = name;
    }
	 
	static QueryToken reverseToken(QueryToken q_t)
	{
		QueryToken token = q_t;
		switch(q_t)
		{
		case LESS_EQUAL:
			token = QueryToken.GREATE_EQUAL;
			break;
		case LESS_THAN:
			token = QueryToken.GREATE_THAN;
			break;
		case GREATE_EQUAL:
			token = QueryToken.LESS_EQUAL;
			break;
		case GREATE_THAN:
			token = QueryToken.LESS_THAN;
			break;
		}
		
		return token;
	}
}

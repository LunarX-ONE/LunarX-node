
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

 
public enum ParserStatus
{ 
		at_begin 
			{
				public QueryToken nextToken(){return null;}  
			}, 
		at_reading_next_expr
			{
				public QueryToken nextToken(){return null;}  
			}, 
		at_reading_column 
			{
				public QueryToken nextToken(){return null;}  
			}, 
		at_reading_value 
			{
				public QueryToken nextToken(){return null;}  
			}, 
		at_reading_keyword 
			{
				public QueryToken nextToken(){return null;}  
			}, 
		at_reading_keyword_ended 
			{
				public QueryToken nextToken(){return null;}  
			}, 
		at_end 
			{
				public QueryToken nextToken(){return null;}  
			}; 
		public abstract QueryToken nextToken() ; 
	} 

 

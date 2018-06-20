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

 
import java.util.HashMap;
import java.util.Iterator;

import LunarX.RecordTable.StoreUtile.LunarColumn; 

 
public class PatternInterpreter { 
	
	 int status;
	static final int at_begin = 0;
	static final int at_reading_column = 1;
	static final int at_reading_value = 2;
	static final int at_reading_text = 3;
	static final int at_reading_text_ended = 4;
	static final int at_end = 5; 
	
	/*
	 * since record with text column maybe recursive:
	 * {ccc=[" another_rec={col=["what the fuck is this record? "]} "]}
	 */
	//private int in_reading_text_recursive_index = 0; 
	/*
	 * extract :{ppp=vvv, ppp1=vvv1, ppp2=vvv2,...}
	 */
	@Deprecated
	public HashMap<String, LunarColumn> build(String record)
	{
		HashMap<String, LunarColumn> vec = new HashMap<String, LunarColumn>();
		status = at_begin;
		
		final int end = record.length();
	 
		int start_at = 0;
		int end_at = 0;
		String[] col_val = new String[2];
		for (int i = 0; i < end; i++) 
    	{ 
			final char code = record.charAt(i);
			switch(code)
			{
			case RecordToken._begin:
				if(! (status == at_begin))
					return vec;
				start_at++;
				end_at++;
				status = at_reading_column;
				break;
			case RecordToken._end:
				if(! (status == at_reading_value))
					return vec;
				col_val[1] = record.substring(start_at, end_at).trim();
				
				end_at ++;				
				start_at = end_at; 
				 
				vec.put(col_val[0], new LunarColumn(col_val));
				col_val = new String[2];
				
				status = at_end;
			case RecordToken._evaluate:
				if(! (status == at_reading_column))
					return vec;
				//System.arraycopy(record, start_at, col_val[0], 0, end_at-start_at+1);
				
				col_val[0] = record.substring(start_at, end_at).trim();
				end_at ++;
				start_at = end_at;
				status = at_reading_value;
				break;
			case RecordToken._comma:
				if(! (status == at_reading_value))
					return vec;
				
				col_val[1] = record.substring(start_at, end_at).trim();
				end_at ++;
				start_at = end_at;
				
				vec.put(col_val[0], new LunarColumn(col_val));
				col_val = new String[2];
				
				status = at_reading_column;
				break;
			default:
				if( status == at_begin) 
					start_at++;
				
				end_at ++;
				break;
			}
    	}
		
		return vec;
	}
	
	/*
	 * extract : ccc=vvv, ccc1=vvv1, ccc2=vvv2, ccc3=["abc,sss, sadsda, dasdasas dasdadsd adsdad"], ... 
	 */
	public HashMap<String, LunarColumn> buildRaw(String record )
	{
		return buildRaw( record,0, record.length()-1);
	}
	
	/*
	 * tokens shall be: "=[\"", "\"]", "{", "}", ":=",..........
	 * source may be any string. 
	 * this function seek the next token from begin_at, and return the index where the token finishes.
	 * 
	 *  Ex. token="[\"", source = "name=   [  \" abd, cdd", begin at 8,  
	 *  returns 11, where the token finishes: 
	 */
	static int confirmToken(String token, String source, int begin_at)
	{
		if(begin_at >= source.length())
			return -1;
		
		char begin_char_at = source.charAt(begin_at);
		int real_start = begin_at;
		while(begin_char_at == RecordToken._nothing)
		{
			real_start++;
			if(real_start == source.length())
				return -1; 
		}
		
		int start = real_start;
		int token_at = 0;
		char char_at = source.charAt(start);
		if(char_at != token.charAt(token_at))
		{
			/*
			 * the first char does not match, then nothing to confirm.
			 */
			return -1;
		}
		token_at++;
		if(token_at >= token.length())
			return start; 
		
		start++;
		if(start == source.length())
			return -1;
		
		 
		while(token_at < token.length())
		{
			if(start == source.length())
				return -1;
			char_at = source.charAt(start);
			while(char_at == RecordToken._nothing)
			{
				start++;
				if(start == source.length())
					return -1;
				else
					char_at = source.charAt(start);
			}
			if(char_at == token.charAt(token_at))
			{
				token_at ++;
				start ++;
			}
			else
				return -1;
		}
		
		return start-1;
		
			
	}
	
	static boolean confirmNumber(String str)
	{
		for(int i=0;i<str.length();i++)
		{
			if(!java.lang.Character.isDigit(str.charAt(i)))
				return false; 
		}
		return true;
	}
	
	/*
	 * Be noticed that the java String.substring(...) has the ending index, exclusive.
	 */
	public HashMap<String, LunarColumn> buildRaw(String record, int start_index, int end_index)
	{
		HashMap<String, LunarColumn> vec = new HashMap<String, LunarColumn>();
		status = at_reading_column; 
	 
		int start_at = 0;
		int end_at = 0;
		 
		LunarColumn col_val = new LunarColumn();
		for (int i = start_index; i <= end_index; i++) 
    	{ 
			final char code = record.charAt(i);
			switch(code)
			{
			 
			case RecordToken._evaluate:
				if( status == at_reading_text)
				{
					end_at++;
				}
				else
				{
					if(! (status == at_reading_column))
						return vec;
					//System.arraycopy(record, start_at, col_val[0], 0, end_at-start_at+1);
					 
					int text_start_at = confirmToken(RecordToken._begin_text, record, i );
					/*
					 * it is not a beginning of reading text
					 */
					if(text_start_at == -1)
					{ 
						col_val.setColumnName(record.substring(start_at, i).trim());
						col_val.setBeginAt(start_at);
						end_at ++;
						 
						start_at = i+1;
						 
					 
						status = at_reading_value;
					}
					else /* it is a beginning of reading text */
					{ 
						col_val.setColumnName(record.substring(start_at, i).trim());
							col_val.setBeginAt(start_at);
							
							i = text_start_at;
							start_at = text_start_at+1;
							end_at = start_at;
							status = at_reading_text;	 
						 
					} 
				}
				
				break;
			case RecordToken._possible_end_text:
				if( status != at_reading_text)
				{
					end_at++;
				}
				else
				{
					int token_end_at = confirmToken(RecordToken._end_text, record, i);
					/*
					 * it is not a token of ending text.
					 */
					if(token_end_at == -1)
					{
						end_at ++;
					}
					else
					{ 
						 
							col_val.setColumnValue(record.substring(start_at, i).trim());
							col_val.setEndAt(token_end_at);
						
							end_at = token_end_at;
							start_at = token_end_at;
							i = token_end_at;
						
							//vec.put(col_val[0], new LunarColumn(col_val));
							//col_val = new String[2];
							vec.put(col_val.getColumnName(), col_val);
							 
							col_val = new LunarColumn();
							status = at_reading_text_ended ;
							
							 
						 
						 
					}
					
				}
				break;
			
			case RecordToken._comma:
				if( status == at_reading_text)
				{
					end_at++;
				}
				else if(status == at_reading_text_ended)
				{					
					col_val.setColumnValue(record.substring(start_at, i).trim());
					col_val.setEndAt(i-1);
					start_at = i+1;
					end_at = start_at;
					
					status = at_reading_column;
				}
				else
				{
					if(! (status == at_reading_value))
					{
						 
						return vec;
					}
					//col_val[1] = record.substring(start_at, end_at).trim();
					col_val.setColumnValue(record.substring(start_at, i).trim());
					col_val.setEndAt(i-1);
				
					end_at = i+1;
					start_at = i+1;
				
					//vec.put(col_val[0], new LunarColumn(col_val));
					//col_val = new String[2];
					vec.put(col_val.getColumnName(), col_val);
					col_val = new LunarColumn();
					status = at_reading_column;
				}
				break;
			 
			default: 
				if(end_at == end_index)
				{ 
					col_val.setColumnValue(record.substring(start_at, end_at+1).trim());
					col_val.setEndAt(end_at);
					
					vec.put(col_val.getColumnName(), col_val);
				}
				end_at ++;
				break;
			}
    	}
		
		return vec;
	}
	
	@Deprecated
	public int[] extractColumn(String record, String column )
	{
		int[] start_end = new int[2];
		start_end[0]=-1;
		start_end[1]=-1;
		
		final int end = record.length();
	 
		String column__ = " "+column;
		int start_at = record.indexOf(column__);
		
		status = at_reading_column;
		
		int end_at = start_at;
		String col ; 
		for (int i = start_at; i < end  ; i++) 
    	{ 
			final char code = record.charAt(i);
			switch(code)
			{
			 
			case RecordToken._evaluate:
				if(! (status == at_reading_column))
					return start_end; 
				
				col = record.substring(start_at, end_at).trim();
				if(col.equalsIgnoreCase(column__))
				{
					start_end[0] = start_at;
					end_at ++;
					start_at = end_at;
					status = at_reading_value;
				}
				else 
				{
					start_at = record.indexOf(column__, end_at) ; 
					end_at = start_at;
					status = at_reading_column;
					
				}
				break;
			case RecordToken._comma:
				if(! (status == at_reading_value))
					return start_end; 
				 
				start_end[1] = end_at-1;  
				
				return start_end; 
			default:
				 
				end_at ++;
				if(end_at == end)
				{  
					if(start_end[0] == -1)
						return start_end;
					else
					{
						start_end[1] = end_at-2;
						return start_end; 
					}
				}
				break;
			}
    	}
		
		return start_end;
	}
	
	public  void testRaw( )
	{
		//String rec_raw = "  name    =   jacksuu    uuuuuuon5  , name= michael , paymenttttt=500, age=3666666660, date = 20161212    ,  ge   = 12345678   " ;
		//String rec_raw2 = "name    =jacksuu    uuuuuuon5  , name= michael , paymenttttt=500, age=3666666660, date = 20161212    ,  ge   = 12345678" ;
		//String rec_raw = "name    =jacksuu" ;
		
		//String rec_raw = "name    =jacksuu" ;
		
		
		String rec_raw = " name    =   jacksuu    uuuuuuon5  , text2= [\"michael]= ok, , is good\"   ]   , paymenttttt=500, text=[\" what the fuck! \"], date=1234 ";
		//String rec_raw = "name=jackson6, score=50, comment=[\"一年一年可以看到更富更强些。而这个富，是共同的富，这个强，是共同的强，大家都有份,,.  \"]";
		
		//String rec_raw = "name=jackson6, score=[\" another=[\" another recursive col \"] \"], comment=[\"一年一年可以看到更富更强些。而这个富，是共同的富，这个强，是共同的强，大家都有份,,.  \"]";
		
		//HashMap<String, LunarColumn> cols = extract(rec);
		HashMap<String, LunarColumn> cols = buildRaw(rec_raw, 0, rec_raw.length()-1);
		Iterator<String> keys =  cols.keySet().iterator();
		while(keys.hasNext())
		{
			LunarColumn lc = cols.get(keys.next());
			System.out.println("column: " + lc.getColumnName());
			System.out.println("value: " + lc.getColumnValue());
			
		}
		
		//int[] pos = extractColumn(rec_raw, "ge");
		
		//System.out.println("start_at: " + pos[0]);
		//System.out.println("end at: " + pos[1]);
		//String col_name = "name";
		String col_name = "text2";
		if(cols.get(col_name)!=null)
		{
			System.out.println("start_at: " + cols.get(col_name).getBeginAt());
			System.out.println("end at: " + cols.get(col_name).getEndAt());
			/*
			 * substring(...) has the ending index, exclusive.
			 */
			System.out.println("the column is: " + rec_raw.substring(
												cols.get(col_name).getBeginAt(),
												cols.get(col_name).getEndAt()+1));
		}
		
	}
	
	public void testConfirmToken()
	{
		String token = "=[\"";
		//String source = "name : =[\" what the fuck!!, \"";
		String source = "name : = [   \"   [= ";
		
		int index = 0;
		while(source.charAt(index)!= token.charAt(0))
			index++;
		
		int token_end_at = confirmToken(token, source, index);
		System.out.println(token_end_at);
	}
	
	public static void main(String[] args) {
		
		PatternInterpreter pi = new PatternInterpreter(); 
		pi.testRaw( );
		//pi.testConfirmToken();
	}
		
}

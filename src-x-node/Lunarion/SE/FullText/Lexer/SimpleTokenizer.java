package Lunarion.SE.FullText.Lexer;

import java.util.HashMap;

import Lunarion.SE.AtomicStructure.TermScore;

public class SimpleTokenizer extends TokenizerInterface{

	private final String token_splitter = " ";
	
	public HashMap<String, TermScore> tokenizeTerm(String input_str)
	{
		String[] temp_str = input_str.split(token_splitter);
		HashMap<String, TermScore> hash = new HashMap<String, TermScore>();
		for(int i=0;i<temp_str.length;i++)
		{
			if(hash.get(temp_str[i]) == null)
				hash.put(temp_str[i], new TermScore(temp_str[i], 0));
			else
			{
				TermScore exist_term = hash.get(temp_str[i]);
				int new_score = exist_term.getScore()+1;
				exist_term.setScore(new_score);
				hash.put(temp_str[i], exist_term);
			}
		}
		
		return hash;
	}
}

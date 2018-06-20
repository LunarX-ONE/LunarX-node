package Lunarion.SE.FullText.Lexer;

import java.util.HashMap;

import Lunarion.SE.AtomicStructure.TermScore;

public class NoTokenizer extends TokenizerInterface{

	 
	public HashMap<String, TermScore> tokenizeTerm(String input_str)
	{  
		HashMap<String, TermScore> hash = new HashMap<String, TermScore>(); 
		hash.put(input_str.trim(), new TermScore(input_str, 0)); 
		
		return hash;
	}
}

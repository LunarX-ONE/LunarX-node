package Lunarion.SE.FullText.Lexer;

import java.util.HashMap;

import Lunarion.SE.AtomicStructure.TermScore;

public abstract class TokenizerInterface {
	
	abstract public HashMap<String, TermScore> tokenizeTerm(String content);

}

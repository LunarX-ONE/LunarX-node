package Lunarion.SE.AtomicStructure;

public class TermScore {
	
	private int score;
	private final String term;
	
	public TermScore(String __term,  int __score)
	{
		this.score = __score;
		this.term = __term;
	}
	
	public String getTerm()
	{
		return this.term;
	}
	
	public int getScore()
	{
		return this.score;
	}
	
	public int setScore(int new_score)
	{
		return this.score = new_score;
	}

}

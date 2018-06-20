package LunarX.Node.Grammar.AST.Relational;

public class RelConstants {
	
	public enum AlgebraicRelDef {
		AND,
		OR,
		RANGE,
		FT
	}
	
	
	public final static char start_exp = '[';
	public final static char end_exp = ']';
	public final static char seperator = ',';
	public final static char start_and = 'A';
	public final static char start_or = 'O';
	public final static char start_range = 'R'; /* for range query */
	public final static char start_ft_search = 'T'; /* for fulltext search*/
}

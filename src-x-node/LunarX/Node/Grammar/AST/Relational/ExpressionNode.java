package LunarX.Node.Grammar.AST.Relational;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import LunarX.Node.API.LunarXNode;
import LunarX.Node.API.Result.FTQueryResult;
import LunarX.Node.Grammar.AST.Relational.RelConstants.AlgebraicRelDef;
import LunarX.RecordTable.StoreUtile.Record32KBytes; 

/*
 * e.g. A[
 *         O[R[col1, 0, 50, 0,0], 
 *         R[col2, -1000, 1500, 0,1]], 
 *         T[col3 against(\"keyword1 + keyword2, keyword3\")],
 *         R[col4, 1000, 2000, 1,1]
 *        ]
 * means:
 * (0<col1<50 OR -1000<col2<=1500) 
 * AND 
 * ("col3 against(\"keyword1 + keyword2, keyword3\")") 
 * AND 
 * (1000<=col4<=2000)
 * 
 * The FORM is one of the following four forms: 
 * A[node, node, node,....node], 
 * O[node, node, node,....node], 
 * R[col, lower, upper, lower_inclisive, upper_inclusive],
 * T[statement like: col3 against(\"keyword1 + keyword2\")]
 * 
 * nothing more.
 * 
 * each node is a FORM.
 * 
 * Tested @TestRelationalExpression
 */
public class ExpressionNode {
	
	/*
	 * true: AND, OR
	 * false: RANGE, FULLTEXT
	 */
	boolean is_algebraic_logic = true; 
	
	/*
	 * for range node and fulltext node
	 */
	String sub_exp;
	 
	RelConstants rel_c = new RelConstants();
	
	final String express_str;
	AlgebraicRelDef my_relation_op;
	int cur_index = 0;
	
	List<ExpressionNode> nodes = null;
	
	public ExpressionNode(String exp_str, int start_at)
	{
		express_str = exp_str;
		cur_index = start_at;
	}
	
	public AlgebraicRelDef getOperation()
	{
		return this.my_relation_op;
	}
	private boolean ignoreSpace()
	{
		while(express_str.charAt(cur_index) == ' ' && cur_index < express_str.length())
			cur_index++;
		
		if(cur_index ==  express_str.length())
			return false;
		
		return true;
	}
	
	public List<ExpressionNode> getNodes()
	{
		return this.nodes;
	}
	
	public String[] getQueryParams()
	{
		if(my_relation_op == AlgebraicRelDef.RANGE )
		{
			String[] params = this.sub_exp.split(""+RelConstants.seperator);
			for(int i=0;i<params.length;i++)
				params[i] = params[i].trim();
			return params; 
		}
		if( my_relation_op == AlgebraicRelDef.FT )
		{
			String[] ft_query = new String[1];
			ft_query[0] = this.sub_exp.trim();
			return ft_query;
		}
		
		return null;
	}
	public String toString()
	{
		switch(my_relation_op)
		{
		case AND:
			return "AND";
		case OR:
			return "OR"; 
		case RANGE:
		case FT:
			return sub_exp; 
		}
		
		return "";
	}
	public int extract( )
	{
		if(!ignoreSpace())
			return -1; 
		char at = express_str.charAt(cur_index);
		switch(at)
		{
		case RelConstants.start_and:
		case RelConstants.start_or:
			{
				if(at == rel_c.start_and)
					my_relation_op = AlgebraicRelDef.AND;
				if(at == rel_c.start_or)
					my_relation_op = AlgebraicRelDef.OR;
				
				cur_index++;
				if(!ignoreSpace())
					return -1;
				
				char next = express_str.charAt(cur_index); 
				if(next != rel_c.start_exp)
					return -1;
				cur_index++;
				
				nodes = new ArrayList<ExpressionNode>();
				while(express_str.charAt(cur_index) != rel_c.end_exp)
				{
					if(express_str.charAt(cur_index) == rel_c.seperator 
							|| express_str.charAt(cur_index) == ' ')
					{
						cur_index++;
					}
					else
					{
						ExpressionNode node_i = new ExpressionNode(express_str, cur_index);
						cur_index = node_i.extract();
						if(cur_index < 0)
							return -1;
						else
						{
							nodes.add(node_i);
							cur_index++;
						}						
					}
				} 
			}
			break;
		case RelConstants.start_range:
		case RelConstants.start_ft_search:
			{ 
				if(at == rel_c.start_range)
					my_relation_op = AlgebraicRelDef.RANGE;
				if(at == rel_c.start_ft_search)
					my_relation_op = AlgebraicRelDef.FT;
				
				cur_index++;
				if(!ignoreSpace())
					return -1;
				
				char next = express_str.charAt(cur_index); 
				if(next != rel_c.start_exp)
					return -1;
				cur_index++;
				
				int start_sub_exp = cur_index;
				while(express_str.charAt(cur_index) != rel_c.end_exp)
				{
					cur_index++;
				}
				sub_exp = express_str.substring(start_sub_exp, cur_index );
			}
			break; 
		}
		
		return cur_index;
		
	}
	
	
}

package LunarX.Node.Grammar.AST.Relational.Visitor;

import java.util.ArrayList;
import java.util.List;

import LunarX.Node.API.LunarXNode;
import LunarX.Node.API.Result.FTQueryResult;
import LunarX.Node.Grammar.AST.Relational.ExpressionNode; 

public class ExecutorVisitor {
	
	LunarXNode db_inst;
	String table;
	public ExecutorVisitor(LunarXNode _db, String _table)
	{
		db_inst = _db;
		table = _table;
	}
	
	public FTQueryResult visit(ExpressionNode exp)
	{
		//System.out.println(exp.toString());
		//System.out.println("==========");
		switch(exp.getOperation())
		{
		case AND:
			{
				List<ExpressionNode> sons = exp.getNodes();
				FTQueryResult result; 
				if(sons != null )
				{
					result = visit(sons.get(0));
					for(int i=1;i<sons.size();i++)
					{  
						FTQueryResult result_i = visit(sons.get(i));
						result = result.intersectAnother(result_i);
					}
					
					return result;
				}
				return null; 
			}  
		case OR:
			{
				List<ExpressionNode> sons = exp.getNodes();
				FTQueryResult result; 
				if(sons != null )
				{
					result = visit(sons.get(0));
					for(int i=1;i<sons.size();i++)
					{  
						FTQueryResult result_i = visit(sons.get(i));
						result = result.unionAnother(result_i);
					}
					
					return result;
				}
				return null; 
			}  
		case RANGE:
			{
				String[] params = exp.getQueryParams();
				boolean lower_inclusive = Integer.parseInt(params[3]) == 1 ?true:false; 
				boolean upper_inclusive = Integer.parseInt(params[4]) == 1 ?true:false; 
				
				return db_inst.queryRange(table,  
											params[0], 
											Long.parseLong(params[1]),
											Long.parseLong(params[2]),
											lower_inclusive,
											upper_inclusive); 
				
			}
		case FT:
			{
				String[] params = exp.getQueryParams();
				return db_inst.queryFullText(table, params[0] , 0);
			} 
		}
		
		return null;
		
	}

}

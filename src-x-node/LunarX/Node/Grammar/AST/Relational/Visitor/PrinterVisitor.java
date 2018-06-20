package LunarX.Node.Grammar.AST.Relational.Visitor;

import java.util.ArrayList;
import java.util.List;

import LunarX.Node.Grammar.AST.Relational.ExpressionNode;
 

 
public class PrinterVisitor {
	
	
	public PrinterVisitor()
	{
		
	}
	
	public void visit(ExpressionNode exp)
	{
		System.out.println(exp.toString());
		System.out.println("==========");
		
		List<ExpressionNode> sons = exp.getNodes();
		if(sons != null )
		{
			for(int i=0;i<sons.size();i++)
			{
				System.out.println("for " + exp.toString() + ": ");
				visit(sons.get(i));
			}
		}
	}

}

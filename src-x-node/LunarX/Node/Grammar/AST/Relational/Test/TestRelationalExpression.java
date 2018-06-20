package LunarX.Node.Grammar.AST.Relational.Test;

import java.io.IOException;
import java.util.ArrayList;

import LunarX.Node.API.LunarXNode;
import LunarX.Node.API.Result.FTQueryResult;
import LunarX.Node.Grammar.AST.Relational.ExpressionNode;
import LunarX.Node.Grammar.AST.Relational.Visitor.ExecutorVisitor;
import LunarX.Node.Grammar.AST.Relational.Visitor.PrinterVisitor;
import LunarX.RecordTable.StoreUtile.Record32KBytes; 

public class TestRelationalExpression {

	public static void testPrint()
	{
		String exp = " A[O[R[col1, 0, 50, 0,0], R[col2, -1000, 1500, 0,1]], T[col3 against(\"keyword1 + keyword2, keyword3\")],R[col4, 1000, 2000, 1,1]]";
		ExpressionNode e_node = new ExpressionNode(exp,0);
		e_node.extract();
		
		PrinterVisitor pv = new PrinterVisitor();
		
		pv.visit(e_node);
	}
	
	public static void testExecutor()
	{
		String db = "/home/feiben/DBTest/RTSeventhDB";
		LunarXNode db_inst = new LunarXNode();
		db_inst.openDB(db);
		String table = "table_for_calcite";
		
		//S.\"score\" between 75 and 98 or S.\"score\" < 50
		String exp = "O[R[score,75,98,1,1], R[score, -10000, 50,0,0]]";
		ExpressionNode e_node = new ExpressionNode(exp,0);
		e_node.extract();
		
		ExecutorVisitor ev = new ExecutorVisitor(db_inst,table);
		
		FTQueryResult result = ev.visit(e_node);
		
		if(result != null && result.resultCount()>0)
		{
			ArrayList<Record32KBytes> list;
			try {
				list = result.fetchRecords(100);
				for(int i=0;i<list.size();i++)
					System.out.println(list.get(i).recData());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args)
	{ 
		testPrint();
		//testExecutor();
	}

}

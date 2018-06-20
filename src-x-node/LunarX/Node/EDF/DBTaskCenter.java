package LunarX.Node.EDF;

import java.io.IOException;

import LCG.EnginEvent.EventDispatcher;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.Delete;
import LunarX.Node.EDF.Events.IncommingRecords;
import LunarX.Node.EDF.Events.OrderedSets;
import LunarX.Node.EDF.Events.QueryAnd;
import LunarX.Node.EDF.Events.QueryLatestN;
import LunarX.Node.EDF.Events.QueryRange;
import LunarX.Node.EDF.Events.QueryRecs;
import LunarX.Node.EDF.Events.QueryResult;
import LunarX.Node.EDF.Events.QuerySimple;
import LunarX.Node.EDF.Events.QuerySimpleIDs;
import LunarX.Node.EDF.Events.Update;
import LunarX.Node.SetUtile.SetOperation; 

public class DBTaskCenter extends EventDispatcher{
	 
	final String db_name;
	final LunarXNode l_db = new LunarXNode();
	final SetOperation set_operator;
	
	public DBTaskCenter( String _db_name ) throws IOException {
		db_name = _db_name;
		set_operator = new SetOperation();
		
		if(l_db.openDB( db_name))
		{
			registerHandler(IncommingRecords.class, new TaskInsert(this));
			
			registerHandler(QueryResult.class, new TaskPrint());
			registerHandler(OrderedSets.class, new TaskIntersect(this, set_operator));
			
			registerHandler(QuerySimple.class, new TaskQuery(this));
			registerHandler(QuerySimpleIDs.class, new TaskQueryIDs(this));
			registerHandler(QueryAnd.class, new TaskQueryAnd(this));
			registerHandler(QueryLatestN.class, new TaskLatestN(this));
			registerHandler(QueryRange.class, new TaskQueryRange(this));
			
			registerHandler(QueryRecs.class, new TaskFetchRecs(this));
			
			registerHandler(Delete.class, new TaskDelete(this));
			registerHandler(Update.class, new TaskUpdate(this));
			
		}
	} 
	
	public LunarXNode getActiveDB()
	{
		return l_db;
	}

	 
	 
	public void saveDB() throws IOException
	{
		l_db.save();
	}
	public void shutdownDB() throws IOException
	{
		l_db.closeDB();
	}
	 
}

package LunarX.Node.EDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import LCG.EnginEvent.Event;
import LCG.EnginEvent.EventHandler;
import LCG.EnginEvent.Interfaces.LFuture;
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.IncommingRecords;
import LunarX.Node.EDF.Events.OrderedSets;
import LunarX.Node.EDF.Events.QueryAnd;
import LunarX.Node.EDF.Events.QueryRecs;
import LunarX.Node.EDF.Events.QueryResult;
import LunarX.Node.EDF.Events.QuerySimple;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class TaskFetchRecs implements LHandler<Event, ArrayList<Record32KBytes>> { 
	
	  private final LunarXNode _db_instance;
	  private final DBTaskCenter _db_task_center;
	  
	  public TaskFetchRecs(DBTaskCenter _dbtc) {
		  _db_task_center = _dbtc;
		  this._db_instance = _db_task_center.getActiveDB();
	  } 
	  
	  public ArrayList<Record32KBytes> execute(Event evt) {
	    if (evt.getClass() == QueryRecs.class) {
	    	QueryRecs q = (QueryRecs) evt;
	    	return internalExecute(q);
	    }
	    return null;
	  } 
	 
	  private ArrayList<Record32KBytes> internalExecute(QueryRecs _q) 
	  {
		  try { 
			  return _db_instance.fetchRecords(_q._table, _q._rec_ids);  
		  } catch (  IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		  }  
	  } 

}

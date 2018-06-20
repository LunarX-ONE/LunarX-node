package LunarX.Node.EDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import LCG.EnginEvent.Event;
import LCG.EnginEvent.EventHandler;
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.API.Result.FTQueryResult;
import LunarX.Node.API.Result.RGQueryResult;
import LunarX.Node.EDF.Events.IncommingRecords;
import LunarX.Node.EDF.Events.QueryRange;
import LunarX.Node.EDF.Events.QueryResult;
import LunarX.Node.EDF.Events.QuerySimple;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class TaskQueryRange implements LHandler<Event, FTQueryResult> { 
	
	  private final LunarXNode _db_instance;
	  private final DBTaskCenter _db_task_center;
	  
	  public TaskQueryRange(DBTaskCenter _dbtc) {
		  _db_task_center = _dbtc;
		  this._db_instance = _db_task_center.getActiveDB();
	  } 
	  
	  public FTQueryResult execute(Event evt) {
	    if (evt.getClass() == QueryRange.class) {
	    	QueryRange q = (QueryRange) evt;
	    	return internalExecute(q);
	    }
	    return null;
	  } 
	 
	  private FTQueryResult internalExecute(QueryRange _q) 
	  {

		  return _db_instance.queryRange(_q._table, _q._property, _q._key_start, _q._key_end) ; 
		   
	  } 

}

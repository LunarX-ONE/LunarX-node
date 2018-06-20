package LunarX.Node.EDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import LCG.EnginEvent.Event;
import LCG.EnginEvent.EventHandler;
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.IncommingRecords;
import LunarX.Node.EDF.Events.QueryResult;
import LunarX.Node.EDF.Events.QuerySimple;
import LunarX.Node.EDF.Events.QuerySimpleIDs;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class TaskQueryIDs implements LHandler<Event, int[]> { 
	
	  private final LunarXNode _db_instance;
	  private final DBTaskCenter _db_task_center;
	  
	  public TaskQueryIDs(DBTaskCenter _dbtc) {
		  _db_task_center = _dbtc;
		  this._db_instance = _db_task_center.getActiveDB();
	  } 
	  
	  public int[] execute(Event evt) {
	    if (evt.getClass() == QuerySimpleIDs.class) {
	    	QuerySimpleIDs q = (QuerySimpleIDs) evt;
	    	return internalExecute(q);
	    }
	    return null;
	  } 
	 
	  private int[] internalExecute(QuerySimpleIDs _q) 
	  {  
		  return _db_instance.queryIDs(_q._table, _q._property, _q._value, _q._latest_count); 
	  } 

}

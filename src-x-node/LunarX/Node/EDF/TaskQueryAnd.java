package LunarX.Node.EDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import LCG.EnginEvent.Event;
import LCG.EnginEvent.EventHandler;
import LCG.EnginEvent.Interfaces.LFuture;
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.IncommingRecords;
import LunarX.Node.EDF.Events.OrderedSets;
import LunarX.Node.EDF.Events.QueryAnd;
import LunarX.Node.EDF.Events.QueryResult;
import LunarX.Node.EDF.Events.QuerySimple;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class TaskQueryAnd implements LHandler<Event, Void> { 
	
	  private final LunarXNode _db_instance;
	  private final DBTaskCenter _db_task_center;
	  
	  public TaskQueryAnd(DBTaskCenter _dbtc) {
		  _db_task_center = _dbtc;
		  this._db_instance = _db_task_center.getActiveDB();
	  } 
	  
	  public Void execute(Event evt) {
	    if (evt.getClass() == QueryAnd.class) {
	    	QueryAnd q = (QueryAnd) evt;
	    	return internalExecute(q);
	    }
	    return null;
	  } 
	 
	  private Void internalExecute(QueryAnd _q) 
	  { 
		  if(_q._q_a._table.equals(_q._q_b._table))
		  {
			  try {
				  int[] ids1 = _db_instance.queryIDs(_q._q_a._table, _q._q_a._property, _q._q_a._value, 0);
				  int[] ids2 = _db_instance.queryIDs(_q._q_b._table, _q._q_b._property, _q._q_b._value, 0);
				  
				  if(ids1 == null || ids2 == null)
				  {
					  System.err.println("No results found! Please check your query, and try again.");
					  return null;
				  }
				  
				  /*
				   * default is ascending order.
				   */ 
				  OrderedSets os = new OrderedSets(ids1, ids2, _q._top_count);  
				  LFuture<int[]> result_ids = _db_task_center.dispatch(os);
			  
				  ArrayList<Record32KBytes> records = _db_instance.fetchRecords(_q._q_a._table, result_ids.get());
				  _db_task_center.dispatch(new QueryResult(records ));
				  return null; 
		  		} catch ( IOException e) {
		  			// TODO Auto-generated catch block
		  			e.printStackTrace();
		  			return null;
		  		} 
		  }
		  else
		  {
			  //TODO: JOIN two tables, filter the result
			  return null;
		  }
	  } 

}

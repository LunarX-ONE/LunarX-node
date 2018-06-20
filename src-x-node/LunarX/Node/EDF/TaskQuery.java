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
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class TaskQuery implements LHandler<Event, Void> { 
	
	  private final LunarXNode _db_instance;
	  private final DBTaskCenter _db_task_center;
	  
	  public TaskQuery(DBTaskCenter _dbtc) {
		  _db_task_center = _dbtc;
		  this._db_instance = _db_task_center.getActiveDB();
	  } 
	  
	  public Void execute(Event evt) {
	    if (evt.getClass() == QuerySimple.class) {
	    	QuerySimple q = (QuerySimple) evt;
	    	return internalExecute(q);
	    }
	    return null;
	  } 
	 
	  private Void internalExecute(QuerySimple _q) 
	  {

		  ArrayList<Record32KBytes> result;

		  try {

			  result = _db_instance.query(_q._table, _q._property, _q._value, _q._latest_count);
			
			  QueryResult qr = new QueryResult(result );
			  
			  _db_task_center.dispatch(qr);
			  return null;
			  
			} catch (IOException | InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} 
		   
	  } 

}

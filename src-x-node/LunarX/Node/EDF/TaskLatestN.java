package LunarX.Node.EDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import LCG.EnginEvent.Event;
import LCG.EnginEvent.EventHandler;
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.IncommingRecords;
import LunarX.Node.EDF.Events.QueryLatestN;
import LunarX.Node.EDF.Events.QueryResult;
import LunarX.Node.EDF.Events.QuerySimple;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class TaskLatestN implements LHandler<Event, ArrayList<Record32KBytes>> { 
	
	  private final LunarXNode _db_instance;
	  private final DBTaskCenter _db_task_center;
	  
	  public TaskLatestN(DBTaskCenter _dbtc) {
		  _db_task_center = _dbtc;
		  this._db_instance = _db_task_center.getActiveDB();
	  } 
	  
	  public ArrayList<Record32KBytes> execute(Event evt) {
	    if (evt.getClass() == QueryLatestN.class) {
	    	QueryLatestN q = (QueryLatestN) evt;
	    	return internalExecute(q);
	    }
	    return null;
	  } 
	 
	  private ArrayList<Record32KBytes> internalExecute(QueryLatestN _q) 
	  {
		  try { 
			  
			  return _db_instance.fetchLatestRecords(_q._table, _q._latest_n);
			  //QueryResult qr = new QueryResult(result );
			  //_db_task_center.dispatch(qr);
			  
		  } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	  } 

}

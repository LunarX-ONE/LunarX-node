package LunarX.Node.EDF;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import LCG.EnginEvent.Event;
import LCG.EnginEvent.EventHandler;
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.IncommingRecords;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
//public class TaskInsert extends EventHandler { 
public class TaskInsert implements LHandler<Event, Record32KBytes[]> { 
	
	private final LunarXNode _db_instance;
	private final DBTaskCenter _db_task_center;
	  
	  public TaskInsert(DBTaskCenter _dbtc) {
		  _db_task_center = _dbtc;
		  this._db_instance = _db_task_center.getActiveDB();
	  } 
	  
	  public Record32KBytes[] execute(Event evt) {
	    if (evt.getClass() == IncommingRecords.class) {
	    	IncommingRecords recs = (IncommingRecords) evt;
	    	return internalExecute(recs._table,  recs._records);
	    }
	    return null;
	  } 
	 
	  private Record32KBytes[] internalExecute(String table, String[] __recs) 
	  {
		  return _db_instance.insertRecord(table, __recs); 
			 
		 
	  } 

}

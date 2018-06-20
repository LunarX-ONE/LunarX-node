package LunarX.Node.EDF;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import LCG.EnginEvent.Event;
import LCG.EnginEvent.EventHandler;
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.Delete;
import LunarX.Node.EDF.Events.IncommingRecords;
//public class TaskInsert extends EventHandler { 
public class TaskDelete implements LHandler<Event, Boolean> { 
	
	private final LunarXNode _db_instance;
	private final DBTaskCenter _db_task_center;
	  
	  public TaskDelete(DBTaskCenter _dbtc) {
		  _db_task_center = _dbtc;
		  this._db_instance = _db_task_center.getActiveDB();
	  } 
	  
	  public Boolean execute(Event evt) {
	    if (evt.getClass() == Delete.class) {
	    	Delete del_id = (Delete) evt;
	    	return _db_instance.deleteRecord(del_id._table, del_id._rec_id);  
	    }
	    return null;
	  } 
	 
	 

}

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
import LunarX.Node.EDF.Events.Update;
//public class TaskInsert extends EventHandler { 
public class TaskUpdate implements LHandler<Event, Boolean> { 
	
	private final LunarXNode _db_instance;
	private final DBTaskCenter _db_task_center;
	  
	  public TaskUpdate(DBTaskCenter _dbtc) {
		  _db_task_center = _dbtc;
		  this._db_instance = _db_task_center.getActiveDB();
	  } 
	  
	  public Boolean execute(Event evt) {
	    if (evt.getClass() == Update.class) {
	    	Update update = (Update) evt;
	    	return _db_instance.updateRecord(update._table, update._rec_id, update._prop, update._val);  
	    }
	    return false;
	  } 
	 
	 

}

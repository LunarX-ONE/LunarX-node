package LunarX.Node.EDF;
 

import java.io.IOException;

import LCG.EnginEvent.Event; 
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.OrderedSets;
import LunarX.Node.SetUtile.SetOperation; 

public class TaskIntersect implements LHandler<Event, int[] > { 
  
	 
	private final DBTaskCenter _db_task_center;
	private final SetOperation set_operator;
	  
	  public TaskIntersect(DBTaskCenter _dbtc, SetOperation _set_operator) {
		  _db_task_center = _dbtc;
		  set_operator = _set_operator;
	  } 
	  
	  public int[] execute(Event evt) {
	    if (evt.getClass() == OrderedSets.class) {
	    	OrderedSets sets = (OrderedSets) evt;
	    	return internalExecute(sets._a, sets._b, sets._top_count);
	    }
	    return null;
	  }  
	  
	  private int[] internalExecute(int[] a, int[] b, int top_count) 
	  {	
		  /*
		   * ascending order.
		   */
		  return set_operator.intersectOrderedSets(a, b, top_count);
		  
		  //try {
		//	ArrayList<Record32KBytes> records = _db_instance.fetchRecords(results);
		//	_db_task_center.dispatch(new QueryResult(records ));
			
		//  } catch (IOException e) {
		//	// TODO Auto-generated catch block
		//	e.printStackTrace();
		//  }
	  } 

	
}

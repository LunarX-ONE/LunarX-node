package LunarX.Node.EDF;
 

import java.io.IOException;

import LCG.EnginEvent.Event; 
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.OrderedSets;
import LunarX.Node.EDF.Events.QueryAnd;
import LunarX.Node.EDF.Events.QuerySimple;
import LunarX.Node.SetUtile.SetOperation; 

public class TaskIntersectReverse implements LHandler<Event, int[] > { 
  
	 
	private final DBTaskCenter db_task_center;
	private final SetOperation set_operator;
	
	  public TaskIntersectReverse(DBTaskCenter _dbtc, SetOperation _set_operator) 
	  {
		  db_task_center = _dbtc;
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
		  return set_operator.intersectOrderedSets(a, b, top_count); 
	  } 

	 
}

package LunarX.Node.EDF;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import LCG.EnginEvent.Event;
import LCG.EnginEvent.EventHandler;
import LCG.EnginEvent.Interfaces.LHandler;
import LunarX.Node.API.LunarXNode;
import LunarX.Node.EDF.Events.IncommingRecords;
import LunarX.Node.EDF.Events.QueryResult;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class TaskPrint implements LHandler<Event, Void> { 
	 
	  public TaskPrint( ) {
	    
	  } 
	  
	  public Void execute(Event evt) {
	    if (evt.getClass() == QueryResult.class) {
	    	QueryResult recs = (QueryResult) evt;
	    	 
	    	return internalExecute( recs._results);
	     
	    }
	    return null;
	  } 
	 
	  private Void internalExecute( ArrayList<Record32KBytes> __result) 
	  {
		  if(__result == null || __result.size() ==0 )
		  {
			  System.err.println("No results found! Please check your query, and try again.");
			  return null;
		  }
		  for(int i=0;i<__result.size();i++)
			  System.out.println(__result.get(i).getID() + ": " +__result.get(i).recData());
		  
		  return null;
	  } 

}

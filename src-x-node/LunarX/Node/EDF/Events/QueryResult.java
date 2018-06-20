package LunarX.Node.EDF.Events;

import java.util.ArrayList;

import LCG.EnginEvent.Event;
import LunarX.Node.API.LunarXNode;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class QueryResult extends Event{
	
	final public ArrayList<Record32KBytes> _results; 
	
	public QueryResult(ArrayList<Record32KBytes> rst ) {
		    this._results = rst; 
		    
	} 

}

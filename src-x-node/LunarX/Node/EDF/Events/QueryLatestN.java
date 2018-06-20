package LunarX.Node.EDF.Events;

import java.util.ArrayList;

import LCG.EnginEvent.Event;
import LunarX.Node.API.LunarXNode;

public class QueryLatestN extends Event{
	
	final public String _table;  
	final public int _latest_n;  
	
	public QueryLatestN(String table, int latest_n_recs) {
		this._table = table;
		_latest_n = latest_n_recs; 
	} 

}

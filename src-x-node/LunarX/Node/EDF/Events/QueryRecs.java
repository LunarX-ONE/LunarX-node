package LunarX.Node.EDF.Events;

import java.util.ArrayList;

import LCG.EnginEvent.Event;
import LunarX.Node.API.LunarXNode;

public class QueryRecs extends Event{ 
	final public String _table;  
	final public int[] _rec_ids; 
	
	public QueryRecs(String table, int[] rec_ids) {
		this._table = table;
		_rec_ids = rec_ids;
	} 

}

package LunarX.Node.EDF.Events;

import java.util.ArrayList;

import LCG.EnginEvent.Event;
import LunarX.Node.API.LunarXNode;

public class QueryRange extends Event{ 
	
	final public String _table;  
	final public String _property;
	final public int _key_start;
	final public int _key_end;
	 
	
	public QueryRange(String table, String prop, int key_start, int key_end) {
		this._table = table;
		_property = prop;
		_key_start = key_start;
		_key_end = key_end; 
	} 

}

package LunarX.Node.EDF.Events;

import java.util.ArrayList;

import LCG.EnginEvent.Event;
import LunarX.Node.API.LunarXNode;

public class QuerySimpleIDs extends Event{ 
	
	final public String _table;  
	final public String _property;
	final public String _value;
	final public int _latest_count;
	
	final public String _query;
	
	public QuerySimpleIDs(String table, String prop, String val, int latest_count) {
		this._table = table;
		_property = prop;
		_value = val;
		_latest_count = latest_count;
		_query = _property.trim()+"="+_value.trim();
	} 

}

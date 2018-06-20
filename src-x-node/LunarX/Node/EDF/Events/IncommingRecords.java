package LunarX.Node.EDF.Events;

import LCG.EnginEvent.Event;
import LunarX.Node.API.LunarXNode;

public class IncommingRecords extends Event{
	
	final public String _table;  
	final public String[] _records;
	
	public IncommingRecords(String table, String[] records) {
		this._table = table;
		this._records = records; 
	} 

}

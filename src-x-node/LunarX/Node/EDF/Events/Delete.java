package LunarX.Node.EDF.Events;

import java.util.ArrayList;

import LCG.EnginEvent.Event;
import LunarX.Node.API.LunarXNode;

public class Delete extends Event{
	
	final public String _table;  
	final public int _rec_id;  
	
	public Delete(String table, int rec_id) {
		_table = table;
		_rec_id = rec_id; 
	} 

}

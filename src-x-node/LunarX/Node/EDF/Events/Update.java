package LunarX.Node.EDF.Events;

 
import LCG.EnginEvent.Event;

public class Update extends Event{
	final public String _table;  
	final public int _rec_id;  
	final public String[] _prop;
	final public String[] _val;
	
	public Update(String table, int rec_id, String[] prop, String[] val) {
		this._table = table;
		_rec_id = rec_id; 
		_prop = prop;
		_val = val;
	} 

}

package LunarX.Node.EDF.Events;

import java.util.ArrayList;

import LCG.EnginEvent.Event;
import LunarX.Node.API.LunarXNode;

public class OrderedSets extends Event{
	
	final public int[] _a;
	final public int[] _b;  
	final public int _top_count;  
	
	public OrderedSets(int[] set_a, int[] set_b, int top_count) {
		_a = set_a;
		_b = set_b;
		_top_count = top_count;
	} 

}

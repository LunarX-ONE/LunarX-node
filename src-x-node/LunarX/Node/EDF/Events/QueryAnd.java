package LunarX.Node.EDF.Events;

import java.util.ArrayList;

import LCG.EnginEvent.Event;
import LunarX.Node.API.LunarXNode;

public class QueryAnd extends Event{
	
	final public QuerySimple _q_a;
	final public QuerySimple _q_b;
	final public int _top_count; 
	
	public QueryAnd(QuerySimple query_a, QuerySimple query_b, int top_count) {
		_q_a = query_a;
		_q_b = query_b;
		_top_count = top_count; 
	} 

}

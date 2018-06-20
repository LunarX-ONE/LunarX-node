
/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contacts: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License.
 * The Original Code is:  LunarBase source code 
 * The LunarBase source code is managed by the development team at Lunarion.com.
 * The Initial Developer of the Original Code is the development team at Lunarion.com.
 * Portions created by lunarion are Copyright (C) lunarion.
 * All Rights Reserved.
 *******************************************************************************
 * 
 */
package LunarX.Node.Utils;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class QuickArray<E> {

	 /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     * 
     * copied from java.util.ArrayList
     */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /**
     * Default initial capacity.
     */
    private final int default_capacity= 1 << 10; 
    private final int default_capacity_mask = (~(default_capacity-1)); 
    private final int default_capacity_bit_len = 10;  
   
 
    private ArrayList<Object[]> array_base = new ArrayList<Object[]>(1000);
    
    //private AtomicInteger capacity = new AtomicInteger(default_capacity);
    private int current_element_count =  0 ;
    //private AtomicInteger current_index_of_base = new AtomicInteger(0);
    private int current_index_in_base_i = 0 ;
    
    
    public QuickArray () {
    	Object[] element_data = new Object[default_capacity];
        array_base.add(element_data);
    }
   
   
    public boolean add(E element)
    {
    	int max_base_index = array_base.size()-1;
    	
    	if(current_element_count  < MAX_ARRAY_SIZE)
    	{  
    		if(current_index_in_base_i < array_base.get(max_base_index).length)
    		{
    			array_base.get(max_base_index)[current_index_in_base_i ] = element;
    			current_index_in_base_i++;
    		}
    		else
    		{ 
    			Object[] element_data = new Object[default_capacity];
    			element_data[0] = element;
    			array_base.add(element_data);
    			current_index_in_base_i = 0;
    		} 
    		current_element_count++;
    		
    		return true;
    	}
    	else
    	{
    		return false;
    		
    	}
    }
     
    public int size()
    {
    	return current_element_count ;
    }
    public E get(int element_index) throws Exception
    {
    	if(element_index >= current_element_count )
    		throw new Exception("[EXCEPTION]: visit the index out of boundry of QuickArray.");
    	
    	int index  = ( element_index & (default_capacity_mask) )>> default_capacity_bit_len;
    	 
    			
    	int remainder = element_index - (element_index & (default_capacity_mask));
    	
    	return (E)array_base.get(index)[remainder];
    }
    
    public static void main(String[] args)
    {
    	int total = 6 * (1 << 20);
    	String[] recs = new String[total];
    	for(int i=0; i< total; i++)
    	{
    		recs[i] = i+"";
    	}
    	
    	QuickArray<String> qa = new QuickArray<String>();
    	long start = System.nanoTime();
    	
    	for(int i=0; i< total; i++)
    	{
    		qa.add(recs[i] );
    	}
    	
    	long end = System.nanoTime();
    	
    	System.out.println("QuicArray add " + total + " elements costs:" + (end - start) + " nano seconds.");
    	
    	ArrayList<String> al = new ArrayList<String>();
    	start = System.nanoTime();
    	
    	for(int i=0; i< total; i++)
    	{
    		al.add(recs[i] );
    	}

    	end = System.nanoTime();
    	
    	System.out.println("ArrayList add " + total + " elements costs:" + (end - start) + " nano seconds.");
    	
    	System.err.println("For adding 1M objects, ArrayList costs less than QuickArray, this fucking implementation failed.");
    	System.err.println("For adding 5M objects, ArrayList costs 3.63 times than QuickArray, this fucking implementation succeeds.");
    	
    }
    
}

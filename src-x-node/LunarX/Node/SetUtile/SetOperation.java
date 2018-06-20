
/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contactor: 
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
package LunarX.Node.SetUtile;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import LunarX.Node.EDF.TaskIntersect;
import LunarX.Node.EDF.TaskIntersectReverse;

public class SetOperation { 

	 /*
	   * a[0,1,2,...n]: smallest --> biggest
	   */
	public int[] intersectOrderedSets(int[] a, int[] b, int top_count) 
	{
		if(a!=null && b != null)
		{ 
			 /*
			   * default is ascending order.
			   */
			  Arrays.sort(a);
			  Arrays.sort(b);
			  
			if (a[0] > b[b.length - 1] || b[0] > a[a.length - 1]) 
				return new int[0];
		
			int[] intersection = new int[Math.max(a.length, b.length)];
			int max_id_a = a.length-1;
			int max_id_b = b.length-1;
			int intersection_max_id = intersection.length-1;
			
			int offset = 0;
			for (int i = 0, b_index = i; i < a.length && b_index < b.length && offset<top_count; i++) 
			{
				/*
				 * a[1,4]
				 * b[2,3]
				 */
				while (a[max_id_a-i] < b[max_id_b-b_index] && b_index < (b.length-1))  
					b_index++;
		 
				if (a[max_id_a-i] == b[max_id_b-b_index]) 
				{ 
					intersection[intersection_max_id - offset] = b[max_id_b - b_index];
					offset++;
					b_index++;
				}
				while (i < (a.length - 1) && a[max_id_a-i] == a[max_id_a - i - 1])  
					i++;
		 
			}
			if (intersection.length == offset) 
			{
				if(intersection.length <= top_count)
					return intersection;
				else
				{
					int[] truncated = new int[top_count];
					System.arraycopy(intersection, intersection.length-top_count, truncated, 0, top_count);
					return truncated;
				}
			}
			int len = Math.min(offset, top_count);
			int[] truncated = new int[len];
			System.arraycopy(intersection, intersection.length-len, truncated, 0, len);
			return truncated;
		  
		}
		else
			return new int[0]; 
	}
	 
	 /*
	   * a[0,1,2,...n]: biggest --> smallest 
	   */
	public int[] intersectReverseOrderedSets(int[] a, int[] b, int top_count) 
	{
		  if(a!=null && b != null)
		  { 
		 
			if (a[0] < b[b.length - 1] || b[0] < a[a.length - 1]) 
				return new int[0];
		
			int[] intersection = new int[Math.max(a.length, b.length)];
			int max_id_a = a.length-1;
			int max_id_b = b.length-1;
			int intersection_max_id = intersection.length-1;
			
			int offset = 0;
			for (int i = 0, b_index = i; i < a.length && b_index < b.length && offset<top_count; i++) 
			{
				/*
				 * a[4,1]
				 * b[3,2]
				 */
				while (a[max_id_a-i] > b[max_id_b-b_index] && b_index < (b.length-1))  
					b_index++;
		 
				if (a[max_id_a-i] == b[max_id_b-b_index]) 
				{ 
					intersection[intersection_max_id - offset] = b[max_id_b - b_index];
					offset++;
					b_index++;
				}
				while (i < (a.length - 1) && a[max_id_a-i] == a[max_id_a - i - 1])  
					i++;
		 
			}
			if (intersection.length == offset) 
			{
				if(intersection.length <= top_count)
					return intersection;
				else
				{
					int[] truncated = new int[top_count];
					System.arraycopy(intersection, intersection.length-top_count, truncated, 0, top_count);
					return truncated;
				}
			}
			int len = Math.min(offset, top_count);
			int[] truncated = new int[len];
			System.arraycopy(intersection, intersection.length-len, truncated, 0, len);
			return truncated;
		}
		else
			  return new int[0];
	  }
  
	
	
	 
	/*
	 * <id, score>
	 * if one id appears in both arrays, it has a higher score.
	 * Here for the simplicity, the score is the times an id appears.
	 * 
	 * Any array has no duplicated ids. 
	 * This algorithm just ignores the duplicated ids.
	 */
	public HashMap<Integer, Integer> union(int[] a, int[] b ) 
	{
		if(a == null || b == null)
		{
			int[] array = (a==null?b:a);
			
			if(array!=null)
			{	
				HashMap<Integer, Integer> result = new HashMap<Integer,Integer>();
				for(int i=0; i<array.length; i++)
					result.put(array[i], 1);
				
				return result;
			}	
			else
				return null;
		}
		else
		{
			HashMap<Integer, Integer> result = new HashMap<Integer,Integer>();
			for(int i=0;i<a.length;i++)
				result.put(a[i], 1);
			
			for(int j=0;j<b.length;j++)
			{
				Integer score = result.put(b[j], 0);
				if(score != null)
				{
					result.put(b[j], score + 1);
				}
			} 
			return result; 
		}
	}
	 
	/*
	 * <id, score>
	 */
	public HashMap<Integer, Integer> union(HashMap<Integer, Integer> a, 
											HashMap<Integer, Integer>  b ) 
	{
		if(a == null || b == null)
		{
			/*
			 * if both null, returns null;
			 */
			return (a==null?b:a);  
		}
		else
		{ 
			Iterator<Entry<Integer, Integer>> it;
			HashMap<Integer, Integer> main;
			it = a.size()<b.size()? a.entrySet().iterator()
					:b.entrySet().iterator();
			
			main = a.size()<b.size()? b:a;
			
			 
			while(it.hasNext())
			{
				Entry<Integer, Integer> entry = it.next();
				Integer score = main.put(entry.getKey(),entry.getValue()) ;
				if(score != null)
				{
					main.put(entry.getKey(), score + 1);
				} 
			}
			
			return main; 
		}
	}
	
	/*
	 * <id, score>
	 */
	public HashMap<Integer, Integer> union(int[] a, HashMap<Integer, Integer>  b ) 
	{
		if(a == null )
		{
			return  b ;  
		}
		else
		{
			if(b==null)
			{
				HashMap<Integer, Integer> result = new HashMap<Integer,Integer>();
				for(int i=0;i<a.length;i++)
					result.put(a[i], 0);
				
				return result;
			}
			else
			{
				for(int i=0;i<a.length;i++)
				{
					int score = 0;
					if(b.get(a[i])!=null)
						score = b.get(a[i])+1; 
					b.put(a[i], score);
				} 
				return b; 
			}
			
		}
	}
	
	/*
	 * <id, score>
	 */
	public HashMap<Integer, Integer> intersect(int[] a, 
											HashMap<Integer, Integer>  b ) 
	{
		/*
		 * if any one is null, the intersect is null
		 */
		if(a == null || b == null || a.length == 0 || b.keySet().size()==0)
		{
			return  null;  
		}
		else
		{ 
			HashMap<Integer, Integer> result = new HashMap<Integer,Integer>();
			for(int i=0;i<a.length;i++)
			{
				if(b.get(a[i]) != null)
					result.put(a[i], 0);
			}
				
			return result; 
			
		}
	}
	
	/*
	 * <id, score>
	 */
	public HashMap<Integer, Integer> intersectSets(int[] a, int[]  b ) 
	{
		
		if(a == null || b == null || a.length==0 || b.length==0)
		{
			return  null;  
		}	
		else
		{ 
			HashMap<Integer, Integer> result = new HashMap<Integer,Integer>();
			
			HashMap<Integer, Integer> a_hash = new HashMap<Integer, Integer> ();
			HashMap<Integer, Integer> b_hash = new HashMap<Integer, Integer> ();
			for(int i=0;i<a.length;i++)
			{
				a_hash.put(a[i], 0);
			}
			
			for(int i=0;i<b.length;i++)
			{
				if(a_hash.get(b[i]) != null)
					result.put(b[i], 1);
			}
			return result;  
		}
	}
	
	/*
	 * <id, score>
	 */
	public HashMap<Integer, Integer> intersectSets(HashMap<Integer, Integer> a, 
														HashMap<Integer, Integer> b)
	{
		if(a!=null && b != null && a.keySet().size()>0 && b.keySet().size()>0)
		{
			Iterator<Entry<Integer, Integer>> it;
			HashMap<Integer, Integer> main;
			it = a.size()<b.size()? a.entrySet().iterator()
					:b.entrySet().iterator();
			
			main = a.size()<b.size()? b:a;
			
			HashMap<Integer, Integer> result = new HashMap<Integer,Integer>();
			
			while( it.hasNext())
			{
				Entry<Integer, Integer> entry =  it.next();
				Integer score = main.get(entry.getKey() ) ;
				if(score != null)
				{
					result.put(entry.getKey(), score + 1);
				} 
			}
			return result;
		}
		else
			  return null;
	  }
  
	  public static void testIntersect()
	  {
		  SetOperation so = new SetOperation();
		  
		  int[] a = {1,5,6,7,9 };
			int[] b = {2,5,8,9 };
			 
			int[] inter =  so.intersectOrderedSets( a,   b, 100) ;
			for(int i = 0;i< inter.length;i++)
				System.out.println(inter[i]); 
		 
	  }
	  
	  public static void testIntersectRandomSets()
	  {
		  SetOperation so = new SetOperation();
		  
		  int[] a = {1,6,7,8,98,2,5,59 };
		  int[] b = {2,5,8,9 };
		  
		  HashMap<Integer, Integer> a_hash = new HashMap<Integer, Integer> ();
			HashMap<Integer, Integer> b_hash = new HashMap<Integer, Integer> ();
			for(int i=0;i<a.length;i++)
			{
				a_hash.put(a[i], 0);
			}
			
			for(int i=0;i<b.length;i++)
			{
				b_hash.put(b[i], 0);
			}
			HashMap<Integer, Integer> result = so.intersectSets(a_hash, b_hash);
			so.printHashMap(result);
			
	  }
	  
	  public static void testIntersectRandomSets2()
	  {
		  SetOperation so = new SetOperation();
		  
		  int[] a = {1,6,7,8,98,2,5,59 };
		  int[] b = {2,5,8,9 };
		  
		  
		  HashMap<Integer, Integer> result = so.intersectSets(a, b );
		  so.printHashMap(result);
			
	  }
	  
	  public void printHashMap(HashMap<Integer, Integer> map)
	  {
		  Iterator<Entry<Integer, Integer>> it = map.entrySet().iterator();
			while(it.hasNext())
			{
				Entry<Integer, Integer> entry = it.next();
				System.out.println(entry.getKey() + " has score " + entry.getValue());
				
			}
	  }
	  public static void testReverseIntersect()
	  {
		  SetOperation so = new SetOperation();
		  
		  int[] a = {6,5, 4,1};
			int[] b = {5,4,2};
			 
			int[] inter =  so.intersectReverseOrderedSets( a,   b, 100) ;
			for(int i = 0;i< inter.length;i++)
				System.out.println(inter[i]);
		 
	  }
	  
	  public static void testUnion()
	  {
		  SetOperation so = new SetOperation();
		  
		  int[] a = {1,5,6,7,9, 21, 3, 5, 6, 89 };
			int[] b = {2,5,8,9 , 89,134, 358};
			 
			HashMap<Integer, Integer> union =  so.union(a,b) ;
			so.printHashMap(union);
		 
	  }
	  public static void testUnion2()
	  {
		  SetOperation so = new SetOperation();
		  
		  int[] a = {1,5,6,7,9, 21, 3, 5, 6, 89 };
			int[] b = {2,5,8,9 , 89,134, 358};
			
			HashMap<Integer, Integer> a_hash = new HashMap<Integer, Integer> ();
			HashMap<Integer, Integer> b_hash = new HashMap<Integer, Integer> ();
			for(int i=0;i<a.length;i++)
			{
				a_hash.put(a[i], 0);
			}
			
			for(int i=0;i<b.length;i++)
			{
				b_hash.put(b[i], 0);
			}
			HashMap<Integer, Integer> union =  so.union(a_hash,b_hash) ;
			so.printHashMap(union);
		 
	  }
	  
	  public static void testUnion3()
	  {
		  SetOperation so = new SetOperation();
		  
		  int[] a = {1,5,6,7,9, 21, 3, 5, 6, 89 };
			int[] b = {2,5,8,9 , 89,134, 358};
			
			HashMap<Integer, Integer> a_hash = new HashMap<Integer, Integer> ();
			for(int i=0;i<a.length;i++)
			{
				a_hash.put(a[i], 0);
			} 
			HashMap<Integer, Integer> union =  so.union(b, a_hash ) ;
			so.printHashMap(union);
		 
	  }
	  
	public static void main(String[] args) throws IOException {
		SetOperation.testIntersect();
		SetOperation.testReverseIntersect();
		SetOperation.testUnion();
		SetOperation.testUnion2();
		SetOperation.testUnion3();
		System.out.println("================");
		
		SetOperation.testIntersectRandomSets();
		System.out.println("================");
		
		SetOperation.testIntersectRandomSets2();
	}
}

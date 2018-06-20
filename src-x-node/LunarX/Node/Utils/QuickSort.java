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
 

package LunarX.Node.Utils;

public class QuickSort {
	
	 public  void quickSort(int[] arrays, int lenght) {
	 if (null == arrays || lenght < 1) {
          System.out.println("input error!");
            return;
        }
         _quick_sort(arrays, 0, lenght - 1);
     }
 
    private void _quick_sort(int[] arrays, int start, int end) 
    {
    	if(start>=end)
            return; 
        
    	int i = start;
    	int j = end;
    	int value = arrays[i];
    	boolean flag = true;
          while (i != j) {
            if (flag) {
                  if (value > arrays[j]) {
                     swap(arrays, i, j);
                   flag=false;
 
                } else {
                     j--;
                 }
             }else{
                if(value<arrays[i]){
                    swap(arrays, i, j);
                      flag=true;
	                 }else{
                     i++;
                  }
             }
          }
          //snp(arrays);
         _quick_sort(arrays, start, j-1);
         _quick_sort(arrays, i+1, end);
        
      }
 
     public void snp(int[] arrays) {
         for (int i = 0; i < arrays.length; i++) {
             System.out.print(arrays[i] + " ");
         }
         System.out.println();
   }
 
     private void swap(int[] arrays, int i, int j) 
     {
    	 int temp;
    	 temp = arrays[i];
    	 arrays[i] = arrays[j];
    	 arrays[j] = temp;
	 
     }
 
    public static void main(String args[]) {
        QuickSort q = new QuickSort();
        int[] a = { 49, 38, 65,12,45,5 };

        q.quickSort(a,6);
        q.snp(a);
     } 
	 
}
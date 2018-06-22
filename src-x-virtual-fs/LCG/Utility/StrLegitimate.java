/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author DADA database team 
  * The contents of this file are subject to the Lunarion Public License Version 1.0
  * ("License"); You may not use this file except in compliance with the License
  * The Original Code is:  Lunar NoSQL Database source code 
  * The Lunar NoSQL Database source code is based on Lunarion Cloud Platform(solution.lunarion.com)
  * The Initial Developer of the Original Code is the development team at Lunarion.com.
  * Portions created by lunarion are Copyright (C) lunarion.
  * All Rights Reserved.
  *******************************************************************************
 * 
 */

package LCG.Utility;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class StrLegitimate {
	 private static final Set<Character> IllegaleChars = new HashSet<Character>();
	 private static final int DEL = 0x7F;
	 private static final char ESCAPE = '%';
	    
	 static { 
		 IllegaleChars.add('\\');
		 IllegaleChars.add('<');
		 IllegaleChars.add('>');
		 IllegaleChars.add(':');
		 IllegaleChars.add('"');
		 IllegaleChars.add('|');
		 IllegaleChars.add('?');
		 IllegaleChars.add('*');
		 IllegaleChars.add('=');
	    }

	 //static final String regEx="[`~!@#$%^&*()+|':;'//[//].<>/?~！@#￥%……&*（）——+|【】‘；：”“’。，、？]";     
	 static final String regEx="[`~!$%^&*()+|''<>?~！￥%……&*（）——+|【】‘”“’]";     
	 
	 /*/
      * CHaracters and numbers are allowed   
      * String regEx  =  "[^a-zA-Z0-9]";    
      * eliminate all illegal chars      
      */ 
	 static final Pattern   illegal_pattern  =   Pattern.compile(regEx);     
	 
	 public static String purifyStringEn(String name) {
	        int len = name.length();
	        StringBuilder sb = new StringBuilder(len);
	        for (int i = 0; i < len; i++) {
	            char c = name.charAt(i);
	            if (c <= ' ' || c >= DEL || IllegaleChars.contains(c) || c == ESCAPE) {
	                sb.append(ESCAPE);
	                sb.append(String.format("%04x", (int) c));
	            } else {
	                sb.append(c);
	            }
	        }
	        return sb.toString();
	    }

	 public static final String purifyStringCnEn(String str) throws PatternSyntaxException   {        
          
		 Matcher   m   =   illegal_pattern.matcher(str);        
		 return   m.replaceAll("").trim();        
   }        
 
	 
	 private static void deleteFile(File f) {
	        if (!f.delete()) {
	        	System.out.println("Failed to delete file " + f.getAbsolutePath());
	        }
	    }
	 
	 public static String getSuffix(int i)
	 {
		 return new StringBuffer(i).toString();
	 }
	 
	 public static void main(String[] args) {
			String ill_str = "@%$$&@@***: :%^#!{}###\" ..,,,,,[...]//哈//哈/哈 =ceshi ";
			System.out.println(StrLegitimate.purifyStringCnEn(ill_str));
			//System.out.println(StrLegitimate.purifyStringEn(ill_str));
		}

}

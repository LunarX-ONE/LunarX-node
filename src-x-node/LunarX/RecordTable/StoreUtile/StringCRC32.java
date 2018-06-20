package LunarX.RecordTable.StoreUtile;

import java.io.UnsupportedEncodingException;

public class StringCRC32 extends java.util.zip.CRC32{

	 public int getIntCRC32()
	 {
		 //return (int)(this.getValue() & ((1<<31)-1));
		 return (int)(this.getValue());
	 }
	 
	 public static void main(String[] args) throws UnsupportedEncodingException 
	 {
		 StringCRC32 scrc = new StringCRC32();
		 scrc.update("hha habgbgbgbgbgha".getBytes());
		 System.out.println(scrc.getValue()); 
		 System.out.println(scrc.getIntCRC32()); 
		 
		 System.out.println(100 & ((1<<5) -1)); 
		 
		 System.out.println( (long)-667366868 & 0xffffffffL);
		
		 //-341663927, should be 1886161445
		 System.out.println( (long)-341663927 & 0xffffffffL);
		 
		 //name=Rafael5, payment=760, age=36
		 String rec = "name=Rafael5, payment=760, age=36";
		 StringCRC32 scrcccccc = new StringCRC32();
		 
		 scrcccccc.update(rec.getBytes("UTF-8"));
		 System.out.println(rec); 
		 System.out.println(scrcccccc.getValue()); 
		 
		 /*
		  * must reset, otherwise, the following calcs will be wrong
		  */
		 scrcccccc.reset();
		 rec = "{" + rec +"}";
		 rec = rec.substring(1, rec.getBytes("UTF-8").length-1);
		 //StringCRC32 scrdddddd = new StringCRC32();
		 
		 scrcccccc.update(rec.getBytes("UTF-8")); 
		 System.out.println(rec); 
		 System.out.println(scrcccccc.getValue()); 
	 }
}

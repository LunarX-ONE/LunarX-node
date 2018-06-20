/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG
 * 
 * @author LunarBase team, contact: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License
 * The Original Code is:  LunarBase source code 
 * The LunarBase source code is managed by the development team at Lunarion.com.
 * The Initial Developer of the Original Code is the development team at Lunarion.com.
 * Portions created by lunarion are Copyright (C) lunarion.
 * All Rights Reserved.
 *******************************************************************************
 * 
 */
 

package LunarX.RecordTable.StoreUtile;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import LCG.MemoryIndex.IndexTypes.DataTypes;
import LCG.MemoryNative64.ByteArray;
import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import LunarX.Basic.Serializable.Impl.Int2Byte;
import LunarX.Node.EDF.Events.OrderedSets;
import LunarX.Node.Grammar.Parser.PatternInterpreter;
import LunarX.RecordTable.Structure.RecordObject;

public class Record32KBytes extends RecordObject{
	
	 
	 
	private long crc32;
	/*
	 * this version set magic number to 1, 
	 * for future versions, increase it.
	 */
	private byte magic = 1;
	/*
	 * hidden variables for transaction.
	 *  
	 */
	private AtomicInteger s_version = new AtomicInteger(); 
	 
	
	/*
	 * data
	 */
	private Int2Byte s_length; 
	private String s_data; 
	private byte[] data_bytes ;
	private java.util.zip.CRC32 crc_tool = new java.util.zip.CRC32();
	//private StringCRC32 crc_tool = new StringCRC32();
	
	final short  byte_max_length = Short.MAX_VALUE;//= ((1<<15)-1);
	 
	//private String[] p_v_pairs;
	//<column_name, LunarColumn>
	HashMap<String, LunarColumn> columns;
	//private HashMap<String, String> p_v_pairs;
	
	//private byte record_status;  
	PatternInterpreter p_i = new PatternInterpreter();
	
	
	public Record32KBytes(int id) {
		s_data = null;
		data_bytes = null;
		columns = null;
		s_length = new Int2Byte();
		this.rec_id = id;
		
		s_version.set(0);
		s_trx_id = -1;
		s_trx_roll_addr = -1;
		
	}

	/*
	 * must be :{ppp=vvv, ppp1=vvv1, ppp2=vvv2,...}
	 */
	public Record32KBytes(int id, String data) {
		
		this.rec_id = id;
		s_version.set(0);
		
		try {
			//short t_length = (short) Math.min(data.trim().getBytes("UTF-8").length, byte_max_length);
			//s_length = new Int2Byte(t_length);
			if(data.getBytes("UTF-8").length <= byte_max_length )
				this.s_data = data.substring(1, data.length() - 1).trim();
			else
			{
				System.out.println("data length: " + data.length());
				System.out.println("data byte length: " + data.getBytes("UTF-8").length);
				System.out.println(data);
				
				/*
				 * since utf-8 use 1 to 3 bytes to encode one character, 
				 * here divide by 3 to avoid index out of the boundary.
				 */
				this.s_data = data.substring(1, (int)(byte_max_length/3)).trim();
			}
			data_bytes = this.s_data.getBytes("UTF-8");
			s_length = new Int2Byte((short)data_bytes.length);
			//p_v_pairs = this.s_data.split(",");
			columns = p_i.buildRaw(s_data);
			 
			/*
			  * must reset, otherwise, the following calcs will be wrong
			  */
			crc_tool.reset();
			crc_tool.update(data_bytes);
			this.crc32 = crc_tool.getValue(); 
			//this.crc32 = crc_tool.getIntCRC32();
			
			s_trx_id = -1;
			s_trx_roll_addr = -1;
			
		} catch (UnsupportedEncodingException e) {
			s_data = null;
			data_bytes = null;
			columns = null;
			s_length = new Int2Byte();
			rec_id = -1;
			
			s_version.set(0);
			s_trx_id = -1;
			s_trx_roll_addr = -1;
			e.printStackTrace();
		}
		
		
		
	}

	public int getID()
	{
		return this.rec_id;
	}
	
	public int getVersion()
	{
		return this.s_version.get();
	}
	
	public void setTrxRollBack(long roll_pos)
	{
		this.s_trx_roll_addr = roll_pos;
	}
	
	public long getTrxRollBack()
	{
		return this.s_trx_roll_addr;
	}
	
	 
	/*
	 * returns the old column values that are replaced.
	 * if there is no old value for column[j], i.e. old_val[j]=null
	 */
	/*
	public String[] update(String[] column, String[] col_val, String[] old_val) throws UnsupportedEncodingException
	{
		if(column.length != col_val.length)
			return old_val;
		 
		for(int j=0;j<column.length ;j++)
		{
			String new_pv = column[j] +"="+ col_val[j];
			int i = 0;
			 
			while(i<this.p_v_pairs.length) 
			{
				if(this.p_v_pairs[i].trim().startsWith(column[j]+"="))
				{ 
					old_val[j] = p_v_pairs[i].trim().split("=")[1].trim();
					this.s_data = this.s_data.replaceFirst(p_v_pairs[i].trim(), new_pv);
					
					break;
				} 
				i++;
			}
			if(i==this.p_v_pairs.length)
			{
				this.s_data = this.s_data + "," + new_pv; 
				old_val[j] = null;
				
			}
		}
		this.p_v_pairs = this.s_data.split(",");
		 
		crc_tool.reset();
		crc_tool.update(this.s_data.getBytes("UTF-8")); 
		this.crc32 = crc_tool.getValue();
		//this.crc32 = crc_tool.getIntCRC32(); 
		s_length = new Int2Byte((short)this.s_data.getBytes("UTF-8").length);
		
		s_version.getAndIncrement();
		
		return old_val;
	}
	*/
	
	public String[] update(String[] column, String[] col_val, String[] old_val) throws UnsupportedEncodingException
	{
		if(column.length != col_val.length 
				|| column.length != old_val.length
				|| col_val.length != old_val.length)
			return old_val;
		 
		for(int j=0;j<column.length ;j++)
		{
			String new_pv = column[j] +"="+ col_val[j];
			 
			if(this.columns.containsKey(column[j]))
			{
				old_val[j] = this.columns.get(column[j]).getColumnValue();
				  
				int begin_at = this.columns.get(column[j]).getBeginAt();
				int end_at = this.columns.get(column[j]).getEndAt();
				  
				this.s_data = this.s_data.substring(0, begin_at)
								+ new_pv 
								+ (end_at == (this.s_data.length()-1)? 
										"":this.s_data.substring(end_at+1, this.s_data.length()));
				this.data_bytes = this.s_data.getBytes("UTF-8");
				//this.columns.put(column[j], new LunarColumn(column[j], col_val[j]));
				this.columns = p_i.buildRaw(s_data );
				
			}
			else
			{
				old_val[j] = null;
				int begin_at = this.s_data.length()+1;
				int end_at = begin_at+new_pv.length()-1;
				this.s_data = this.s_data + "," + new_pv; 
				this.data_bytes = this.s_data.getBytes("UTF-8");
				LunarColumn lc = new LunarColumn(column[j], col_val[j]);
				lc.setBeginAt(begin_at);
				lc.setEndAt(end_at);
				this.columns.put(column[j], lc);
			}
		}
		//this.p_v_pairs = this.s_data.split(",");
		 
		crc_tool.reset();
		crc_tool.update(data_bytes); 
		this.crc32 = crc_tool.getValue();
		//this.crc32 = crc_tool.getIntCRC32(); 
		s_length = new Int2Byte((short)data_bytes.length);
		
		s_version.getAndIncrement();
		
		return old_val;
	}
	
	public void Read(IOInterface io_v) throws IOException 
	{
		synchronized(this)
		{
			synchronized(io_v)
			{ 
				int id = io_v.read4ByteInt();
				if(id != this.rec_id)
				{
					System.out.println("ID Error: record "+ id +" stored is not the one we seek: " + this.rec_id);
					s_data = null;
					//p_v_pairs = null;
					this.columns = null;
					return;
				}
				crc32 = io_v.ReadLong();
				 
				magic = (byte)io_v.read1ByteInt();
				if(magic != 1)
					System.out.println("Magic Number Error "+ magic +", should be 1 " );
				
				s_version.set(io_v.read4ByteInt()); 
				s_trx_id = io_v.read4ByteInt();
				s_trx_roll_addr = io_v.ReadLong();
				
				s_length.Read(io_v); 
				if ( s_length.Get() != -1) { 
					byte[] content = new byte[s_length.Get()];
					if(-1 != io_v.read(content))
					{
						this.data_bytes = content;
						s_data = new String(content,0,content.length,"UTF-8").trim(); 
						crc_tool.reset();
						crc_tool.update(this.data_bytes);
						if(this.crc32 != crc_tool.getValue())
						{ 
							System.out.println("CRC32 Error: record data may be damaged "+ crc_tool.getValue() +", should be " + this.crc32 );
							s_data = null;
							//p_v_pairs = null;
							this.columns = null;
						}
						else 
							//p_v_pairs = s_data.trim().split(",");
							this.columns = p_i.buildRaw(s_data );
					}
				} else {
					s_data = null;
					//p_v_pairs = null;
					this.columns = null;
				}
			}
		}
	}
 
	
	public void Read(ByteArray io_memory) throws Exception 
	{
		synchronized(this)
		{
			int len = io_memory.getLength();
			//byte[] head = new byte[recHeadLength()]; 
			//byte[] data_bytes = new byte[len - head.length];
			//io_memory.get(head);
			
			//int id = VariableGeneric.Transform4ByteToInt(head, 0);
			int id = io_memory.read4ByteInt(0);
			if(id != this.rec_id)
			{
				System.err.println("[ERROR]: Memory Store Error: ID Error: record "+ id +" stored is not the one we seek: " + this.rec_id);
				s_data = null;
				data_bytes = null;
				//p_v_pairs = null;
				this.columns = null;
				return;
			}
			
			//crc32 = VariableGeneric.Transform8ByteToLong(head, 8);
			//magic = (byte) head[16];
			crc32 = io_memory.read8ByteLong(4);
			magic = io_memory.readByte(12);
			
			if(magic != 1)
				System.err.println("[ERROR]: Magic Number Error "+ magic +", should be 1 " );
				
			//s_version.set(VariableGeneric.Transform4ByteToInt(head, 4));
			//s_trx_id = VariableGeneric.Transform4ByteToInt(head, 17);
			//s_trx_roll_addr = VariableGeneric.Transform8ByteToLong(head, 21);
			s_version.set(io_memory.read4ByteInt(13));
			s_trx_id = io_memory.read4ByteInt(17);
			s_trx_roll_addr = io_memory.read8ByteLong(21);
			
			s_length.Read(io_memory, 29); 
			 
			//if(s_length.Get() == data_bytes.length)
			if(s_length.Get() == (io_memory.getLength()-recHeadLength()))
			{ 
				byte[] __data_bytes = new byte[len - recHeadLength()];
				io_memory.get(recHeadLength(), __data_bytes);
				this.data_bytes = __data_bytes;
				s_data = new String(__data_bytes,0,__data_bytes.length,"UTF-8").trim(); 
				crc_tool.reset();
				crc_tool.update(this.data_bytes);
				if(this.crc32 != crc_tool.getValue())
				{
					System.err.println("[ERROR]: Memory storage Error: CRC32 Error@Record32KBytes.Read(ByteArray io_memory): record data may be damaged "+ crc_tool.getValue() +", should be " + this.crc32 );
					s_data = null;
					this.data_bytes = null;
					//p_v_pairs = null;
					this.columns = null;
				}
				else
					//p_v_pairs = s_data.trim().split(",");
					this.columns = p_i.buildRaw(s_data);
			} 
			else 
			{ 
				s_data = null;
				this.data_bytes = null;
				//p_v_pairs = null;
				this.columns = null;
			} 
		}
	}
 
	
	public void Write(IOInterface io_v) throws IOException {
		synchronized(this)
		{ 
			if (s_data != null) {
				 
					io_v.write4ByteInt(this.rec_id);  
					io_v.WriteLong(crc32);  
					io_v.write(magic);
					
					io_v.write4ByteInt(s_version.get());
					io_v.write4ByteInt(s_trx_id); 
					io_v.WriteLong(s_trx_roll_addr); 
					
					s_length.Write(io_v); 
					//io_v.write(s_data.getBytes("UTF-8"));
					io_v.write(this.data_bytes);
				 
			}
		}
	}
	
	public void Write(ByteArray io_memory) throws Exception {
		synchronized(this)
		{ 
			if (s_data != null) {
				io_memory.put4ByteInt(0, this.rec_id);
				io_memory.put8ByteLong(4, crc32);	 
				io_memory.putByte(12, magic); 
				io_memory.put4ByteInt(13, s_version.get());	 
				io_memory.put4ByteInt(17, s_trx_id);	 
				io_memory.put8ByteLong(21, s_trx_roll_addr);	 
				s_length.Write(io_memory, 29); 
				
				//io_memory.put(31, s_data.getBytes("UTF-8") );
				io_memory.put(31, this.data_bytes );
				 
			}
		}
	}
 
	 
	public int recHeadLength()
	{
		return 4 + 8 + 1 + 4 + 4  + 8 + s_length.Size();
	}
	public int recLength()
	{
		/*
		 * string length + 2bytes size + 
		 */
		return 4 + 8 + 1 + 4 + 4  + 8 + s_length.Size() + s_length.Get() ;
	}
	
	public String recData()
	{
		if(this.s_data == null)
			return null;
		return "{"+this.s_data+"}";
	}
	
	 
	/*
	public String[] getPVPairs()
	{
		return this.p_v_pairs;
	}
	*/
	public HashMap<String, LunarColumn> getColumns()
	{
		return this.columns;
	}
	
	public LunarColumn getColumn(String column_name)
	{
		return this.columns.get(column_name);
	}
	
	public String valueOf( String column)
	{  
		 
		int i = 0;
		String val = "";
		/*
		while(i<this.p_v_pairs.length)
		{
			if(this.p_v_pairs[i].trim().startsWith(column+"="))
			{
				val = this.p_v_pairs[i].split("=")[1].trim();
				return val;
			}
			i++;
		}
		 */
		LunarColumn lc = this.columns.get(column);
		//System.out.println("There is no column "+ column + " in this record: " +  recData());
		if(lc!=null)
			return lc.getColumnValue();
		
		return val; 
		
	}
	public boolean compareTo(Record32KBytes _another_rec, String column, DataTypes _type)
	{ 
		String val = _another_rec.valueOf(column);
		String val_me = this.valueOf(column);
		if(val == "" || val_me == "")
			return true;
		switch(_type)
		{
			case INTEGER:
				if(Integer.parseInt(val) < Integer.parseInt(val_me))
					return true; 
				else
					return false;
			case LONG:
				if(Long.parseLong(val) < Long.parseLong(val_me))
					return true;
				else return false;
			case TIME:
				//SimpleDateFormat sdf = new SimpleDateFormat();
				return false;
		}
		return false;
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		
		Record32KBytes rec = new Record32KBytes(1, "{name=jackson5, payment=500, age=30}");
		
		int len = 5;
		String[] new_prop = new String[len];
		String[] new_val = new String[len];
		String[] old_val = new String[len];
		for(int i=0;i< len ;i++)
		{
			new_prop[i] = "np"+i;
			new_val[i] = "nv"+i;
		}
		
		rec.update(new_prop, new_val, old_val);
		System.out.println(rec.recData());
		
		
		String[] old_prop = new String[2];
		String[] old_prop_val = new String[2];
		String[] replaced_val = new String[2];
				
		old_prop[0] = "payment";
		old_prop_val[0] = "1000000";
		old_prop[1] = "name";
		old_prop_val[1] = "memmememmeme";
		
		rec.update(old_prop, old_prop_val, replaced_val);
		System.out.println(rec.recData());
		System.out.println(replaced_val[0]);
		System.out.println(replaced_val[1]);
		
		
		
		System.out.println("abcdefghijklmn".replaceFirst("de", "fuck") );
		
		 
		
		
	}
}

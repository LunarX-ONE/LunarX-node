package LunarX.Realtime.Column;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import LunarX.Basic.Serializable.Impl.Int2Byte;
import LunarX.Node.EDF.Events.OrderedSets;

public class ValueString {
	
	/*
	 * this value belongs to which record
	 */
	private int rec_id;
	private long crc32;
	/*
	 * this version set magic number to 1, 
	 * for future versions, increase it.
	 */
	private byte magic = 1; 
	
	private AtomicInteger s_version = new AtomicInteger();
	/*
	 * data
	 */
	private Int2Byte byte_length; 
	private String data;
	private byte[] byte_encoded;
	
	 
	
	private java.util.zip.CRC32 crc_tool = new java.util.zip.CRC32();
	
	final short  byte_max_length = Short.MAX_VALUE;//= ((1<<15)-1);
	 
	 
	
	//private byte record_status;
	
	public ValueString( ) {
		data = null;
		byte_encoded = null;
		byte_length = new Int2Byte();
		this.rec_id = -1; 
		s_version.set(0);
		 
	}

	public ValueString(int id, String data, int version) {
		this.rec_id = id;
		this.s_version.set(version);
		//try {
			//short t_length = (short) Math.min(data.getBytes("UTF-8").length, byte_max_length);
			//short t_length = (short) Math.min(data.length(), byte_max_length);
			byte_encoded = VariableGeneric.utf8Encode(data, 0, data.length());
			short t_length = (short) Math.min(byte_encoded.length, byte_max_length);
			
			byte_length = new Int2Byte(t_length);
			//if(data.getBytes("UTF-8").length <= t_length )
			//if(data.length() <= t_length )
			if(byte_encoded.length <= t_length )
				this.data = data;
			else
			{
				//this.data = data.substring(0, t_length);
				this.data = new String(VariableGeneric.utf8Decode(byte_encoded, t_length));
			}
			 
			crc_tool.reset();
			//crc_tool.update(this.data.getBytes());
			crc_tool.update(byte_encoded);
			this.crc32 = crc_tool.getValue(); 
		 
			
		//} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
	}

	public int getRecID()
	{
		return this.rec_id;
	}
	
	public AtomicInteger getVersion()
	{
		return this.s_version ;
	}
	
	public void Read(IOInterface io_v) throws IOException {
		synchronized(this)
		{
			synchronized(io_v)
			{ 
				this.rec_id = io_v.read4ByteInt(); 
				
				crc32 = io_v.ReadLong();
				
				this.s_version.set(io_v.read4ByteInt());
				
				magic = (byte)io_v.read1ByteInt();
				if(magic != 1)
					System.out.println("Magic Number Error "+ magic +", should be 1 " );
			 
				byte_length.Read(io_v); 
				if ( byte_length.Get() != -1) { 
					//byte[] content = new byte[byte_length.Get()];
					//if(-1 != io_v.read(content))
					//char[] content = new char[byte_length.Get()];
					//io_v.readChars(content, 0, byte_length.Get());
					byte_encoded = new byte[byte_length.Get()];
					if(-1 != io_v.read(byte_encoded))
					{
						//data = new String(content,0,content.length,"UTF-8"); 
						 
						//data = new String(content,0, content.length); 
						char[] decoded = VariableGeneric.utf8Decode(byte_encoded, byte_length.Get());
						data = new String(decoded);
						 
						crc_tool.reset();
						//crc_tool.update(this.data.getBytes());
						crc_tool.update(byte_encoded);
						if(this.crc32 != crc_tool.getValue())
						{
							System.out.println("CRC32 Error@ValueString.Read(IOInterface ...): column record data may be damaged "
												+ crc_tool.getValue() 
												+ ", CRC32 should be " 
												+ this.crc32);
							System.out.println("the damaged data maybe: "+data);
							System.out.println("data length is: "+byte_length.Get());
							System.out.println("data id is: "+this.rec_id);
							System.out.println("package @ValueString.Read(IOInterface io_v) 2");
							data = null; 
						} 
					}
				} else {
					data = null; 
				}
			}
		}
	}
 
	public void Write(IOInterface io_v) throws IOException {
		synchronized(this)
		{
			if (data != null) {
				synchronized(io_v)
				{ 
					io_v.write4ByteInt(this.rec_id); 
					io_v.WriteLong(crc32);  
					
					io_v.write4ByteInt(this.s_version.get());
					
					io_v.write(magic);
				 
					byte_length.Write(io_v); 
					
					/*
					 * the following is confusing me:
					 * both using write(byes array) and writeChars(data, 0, data.length()) 
					 * cause incomplete data writing.
					 * Even after encode string into utf-8 byte array 
					 * by VariableGeneric.utf8Encode, whose logic is 
					 * the same of writeChars. 
					 */
					//io_v.write(data.getBytes("UTF-8"));
					//io_v.write(data.getBytes("GBK"));
					//char[] arr = data.toCharArray();
					//io_v.writeChars(arr, 0, arr.length);
					//io_v.writeChars(data, 0, data.length()) ;
					io_v.write(this.byte_encoded);
					
					
				}
			}
		}
	}
 
 
	
	public int recLength()
	{
		/*
		 * string length + 2bytes size + 
		 */
		return 4 + 8 + 4 + 1 +byte_length.Size() + byte_length.Get() ;
	}
	
	public String recData()
	{
		return this.data;
	}
	 
	
  
}

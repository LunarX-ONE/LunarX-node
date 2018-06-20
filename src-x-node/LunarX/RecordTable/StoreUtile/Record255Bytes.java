package LunarX.RecordTable.StoreUtile;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import LCG.StorageEngin.IO.L0.IOInterface;

public class Record255Bytes {
	public String data;
	final static int  byte_max_length = 255;
	private int length;
	public Record255Bytes() {
		data = null;
	}

	public Record255Bytes(String data) {
		this.data = data;
		try {
			length = data.getBytes("UTF-8").length;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void Read(IOInterface io_v) throws IOException {
		synchronized(this)
		{
			synchronized(io_v)
			{
				byte[] byte_buff = new byte[1];
				int byte_length;
				if (io_v.read(byte_buff) != -1) {
					byte_length = ((int) byte_buff[0]) & 0xFF; 
					byte[] content = new byte[byte_length];
					io_v.read(content, 0, byte_length);
					data = new String(content,0,content.length,"UTF-8"); 
				} else {
					data = null;
				}
			}
		}
	}
/*
	public void Write(IOInterface io_v) throws IOException {
		synchronized(this)
		{
			if (data != null) {
				synchronized(io_v)
				{
					int byte_length = Math.min(data.getBytes("UTF-8").length, byte_max_length); 
			
					io_v.write1ByteInt((byte)byte_length);
					io_v.write(data.getBytes("UTF-8"));
				}
			}
		}
	}
*/
	public void Write(IOInterface io_v, String data) throws IOException {
		if (data != null) {
			synchronized(io_v)
			{
				io_v.write((byte) Math.min(data.getBytes("UTF-8").length, byte_max_length));
				io_v.write(data.getBytes("UTF-8"));
			}
		}
	}
	
	public int byteLength()
	{
		return length;
	}
}

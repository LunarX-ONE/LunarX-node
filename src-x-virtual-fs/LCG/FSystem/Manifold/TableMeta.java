package LCG.FSystem.Manifold;
 
import java.io.IOException; 
 
import LCG.FSystem.Def.DBFSProperties;  
import LCG.StorageEngin.IO.L1.IOStreamNative;

public class TableMeta {

	private long lunar_version = 1L;
	 
	private final String lunarmax_file_name; 
	
	private IOStreamNative meta_ios; 
	/*
	 * this is small file with only 2 long-type value to write,
	 * then use 1k bytes for buff. May it be more smaller?
	 */
	private int default_buf_bit_len = 10;

	/*
	 * max_value is used in multiple purposes, 
	 * for IntegerColumnHandlerCenter, it records the max lunarmax id,
	 * for RecordColumnHandlerCenter, it records the max position that has been used by records.
	 */
	private long max_value;
	private boolean is_dirty = false; 
	 
	public TableMeta(String meta_file_name) throws IOException {
		
		lunarmax_file_name = meta_file_name;
		meta_ios = new IOStreamNative(lunarmax_file_name , "rw", default_buf_bit_len);
		
		lunar_version = meta_ios.ReadLong();
		max_value = meta_ios.ReadLong();
	} 

	public void updateMeta() throws IOException
	{
		if(is_dirty)
		{
			meta_ios.seek(0);
			meta_ios.WriteLong(lunar_version);
			meta_ios.WriteLong(max_value);
			meta_ios.flush();
			is_dirty = false;
		}
	}
	
	public long getMaxVal()
	{
		return max_value;
	}
	
	public void setMaxVal(long _max)
	{
		max_value = _max;
		is_dirty = true;
	}
	 
  
	public void close() throws IOException { 
		meta_ios.flush();
		meta_ios.close();
		 
	}
}

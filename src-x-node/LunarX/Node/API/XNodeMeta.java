package LunarX.Node.API;
 
import java.io.IOException; 
 
import LCG.FSystem.Def.DBFSProperties;  
import LCG.StorageEngin.IO.L1.IOStreamNative;

public class XNodeMeta {

	private long db_version = -1L;
	private long owner_thread_id;
	 
	private final String _file_name; 
	
	private IOStreamNative meta_ios; 
	private int default_buf_bit_len ;

	 
	 
	 
	public XNodeMeta(String meta_file_name, int bit_buf_len) throws IOException {
		
		_file_name = meta_file_name + "/ldb.lock" ;
		default_buf_bit_len = bit_buf_len;
		meta_ios = new IOStreamNative(_file_name, "rw", default_buf_bit_len);
		/*
		 * do not seek 0, since if the lock file don't exist, seek(0) will expand the file 
		 * and the default value read will be 0. 
		 * While the -1 is expected, if there is none owner thread existing.
		*/
		//meta_ios.seek(0);
		db_version = meta_ios.ReadLong();
		owner_thread_id = meta_ios.ReadLong();
		 
	} 

	public long getVersion()
	{
		return this.db_version;
	}
	public long getOwner()
	{
		return this.owner_thread_id;
	}
	public void updateMeta(long __version, long __owner_thread_id) throws IOException
	{
		db_version = __version;
		owner_thread_id = __owner_thread_id;
		
		meta_ios.seek(0);
		meta_ios.WriteLong(db_version);
		meta_ios.WriteLong(owner_thread_id);
		meta_ios.flush();
	}
	 
  
	public void close() { 
		try {
			meta_ios.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		meta_ios.close();
		 
	}
}

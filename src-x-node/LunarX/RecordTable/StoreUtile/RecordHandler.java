package LunarX.RecordTable.StoreUtile;

 
import java.io.IOException;
 
import LCG.StorageEngin.IO.L1.IOStreamNative; 
public class RecordHandler extends IOStreamNative {

	//private Record32KBytes record; 

	public RecordHandler(String store_file_name, String mode, int bufbitlen) throws IOException {
		super(store_file_name, mode, bufbitlen);
		//record = new Record32KBytes(); 
	}

	public long insertRecord(Record32KBytes data) throws IOException {
		synchronized (this) {
			//seekEnd();
			//long local_position = length(); 
			data.Write(this);
			return this.GetCurrentPos() - data.recLength() ;
 
			// lock.unlock();
			//return local_position;
		}
	}
	
 
	public void padding(byte[] padding_byts) throws IOException
	{
		synchronized (this) {
			seekEnd();
			 
			this.write(padding_byts); 
		 
		}
	} 
	
	public Record32KBytes readRecord(int rec_id, long position) throws IOException {
		synchronized (this) {
			seek(position);
			Record32KBytes record = new Record32KBytes(rec_id);
			record.Read(this);
			if(record.recData() !=null)
				return record;
			return null;
		}
	}
}

package LunarX.Realtime.Column;

 
import java.io.IOException;
 
import LCG.StorageEngin.IO.L1.IOStreamNative; 
public class ValuesStringHandler extends IOStreamNative { 
 

	public ValuesStringHandler(String store_file_name, String mode, int bufbitlen) throws IOException {
		super(store_file_name, mode, bufbitlen);
		//record = new Record32KBytes(); 
	}

	public long insertValue(ValueString data) throws IOException {
		synchronized (this) {
			//seekEnd();
			//long local_position = length(); 
			data.Write(this);
			return this.GetCurrentPos() - data.recLength() ;
 
			// lock.unlock();
			//return local_position;
		}
	}
	
 
 
	
	public ValueString readValue( long position) throws IOException {
		synchronized (this) {
			seek(position);
			ValueString record = new ValueString( );
			record.Read(this);
			if(record.recData() !=null)
				return record;
			return null;
		}
	}
}

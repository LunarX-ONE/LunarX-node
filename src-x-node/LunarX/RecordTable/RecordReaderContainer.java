package LunarX.RecordTable;

import java.io.File; 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
 
import LCG.FSystem.Def.DBFSProperties;
import LunarX.RecordTable.StoreUtile.Record32KBytes;
import LunarX.RecordTable.StoreUtile.RecordHandler;
import LunarX.RecordTable.StoreUtile.TablePath; 

public class RecordReaderContainer {

	//private List<RecordHandler> reader_list = new ArrayList<RecordHandler>();
	 
	private final RecordHandlerCenter _record_handler_center;
	 
	public RecordReaderContainer(RecordHandlerCenter _rhc) throws IOException {
		
		/*int index = 0;
		while (true) {
			String filename = TablePath.getTableFile() + "." + index;
			File file = new File(filename);
			if (file.exists()) {
				RecordHandler readawdata = new RecordHandler(filename, "r", DBFSProperties.bit_buff_len);
				reader_list.add(readawdata);
				index++;
				// read_raw_data_count++;
			} else {
				break;
			}
		}*/
		_record_handler_center = _rhc;
	}

	public Record32KBytes readRecord(int rec_id, long position) throws IOException {
		long index = position / _record_handler_center.getFSProperties().records_table_file_size;
		long pos = position % _record_handler_center.getFSProperties().records_table_file_size;
		//synchronized (reader_list.get((int) index)) {
		synchronized (this._record_handler_center.getHandler((int) index)) {
			//reader_list.get((int) index).seek(pos); 
			//record.Read(reader_list.get((int) index)); 
			//return record;
			
			return this._record_handler_center.getHandler((int) index).readRecord(rec_id, pos);
		}
	}

	 
}

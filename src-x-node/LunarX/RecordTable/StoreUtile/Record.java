package LunarX.RecordTable.StoreUtile;

import java.io.IOException;

import LCG.StorageEngin.IO.L0.IOInterface;

public class Record {
	public String data;

	public Record() {
		data = null;
	}

	public Record(String data) {
		this.data = data;
	}

	public void Read(IOInterface io_v) throws IOException {
		byte[] byte_buff = new byte[1];
		int length;
		if (io_v.read(byte_buff) != -1) {
			length = ((int) byte_buff[0]) & 0xFF;
			length = byte_buff[0];
			char[] content = new char[length];
			io_v.readChars(content, 0, length);
			data = new String(content);
		} else {
			data = null;
		}
	}

	public void Write(IOInterface io_v) throws IOException {
		if (data != null) {
			io_v.write1ByteInt(data.length());
			io_v.write(data.getBytes());
		}
	}

	public void Write(IOInterface io_v, String data) throws IOException {
		if (data != null) {
			io_v.write((byte) Math.min(data.length(), 255));
			io_v.write(data.getBytes());
		}
	}
}

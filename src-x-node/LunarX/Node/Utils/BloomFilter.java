package LunarX.Node.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;

public class BloomFilter {
	private BitSet bits;
	private String filename;

	public BloomFilter(String filename, int N) {
		this.bits = new BitSet(N);
		this.filename = filename;
	}

	// add element e to bloom filter
	public void add(String e) {
		long[] hashValues = getHashValues(e);
		long size = bits.size();
		for (long v : hashValues) {
			int index = (int) (v % size);
			if (index < 0) {
				// System.out.println("error!!! " + v);
				index = -index;
			}
			bits.set(index);
		}
	}

	// check if element e exists
	public boolean contains(String e) {
		long[] hashValues = getHashValues(e);
		long size = bits.size();
		for (long v : hashValues) {
			int index = (int) (v % size);
			if (index < 0) {
				// System.out.println("error!!! " + v % size);
				index = -index;
			}
			if (!bits.get(index)) {
				return false;
			}
		}
		return true;
	}

	// get element e's three hash values
	private long[] getHashValues(String e) {
		long[] res = new long[3];
		res[0] = GeneralHashFunctions.JSHash(e);
		res[1] = GeneralHashFunctions.ELFHash(e);
		res[2] = GeneralHashFunctions.APHash(e);
		return res;
	}

	/**
	 * save bits to file
	 * 
	 * @param filename
	 *            filename of file to save
	 * @throws IOException
	 */
	public void saveToFile() throws IOException {
		FileOutputStream fos = new FileOutputStream(this.filename);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(bits);
		oos.close();
	}

	/**
	 * load bits from file if the file exists, do not load if the file doesn't
	 * exist
	 * 
	 * @param filename
	 *            load file
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public void loadFromFile() throws IOException {
		File file = new File(this.filename);
		if (file.exists()) {
			FileInputStream fis = new FileInputStream(this.filename);
			ObjectInputStream ois = new ObjectInputStream(fis);
			try {
				bits = (BitSet) ois.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ois.close();
		}
	}
}

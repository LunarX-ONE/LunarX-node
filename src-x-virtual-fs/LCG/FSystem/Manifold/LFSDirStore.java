
/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contacts: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License.
 * The Original Code is:  LunarBase source code 
 * The LunarBase source code is managed by the development team at Lunarion.com.
 * The Initial Developer of the Original Code is the development team at Lunarion.com.
 * Portions created by lunarion are Copyright (C) lunarion.
 * All Rights Reserved.
 *******************************************************************************
 * 
 */
package LCG.FSystem.Manifold;

import java.io.IOException;

import LCG.StorageEngin.IO.L1.IOStreamNative; 

public class LFSDirStore extends IOStreamNative {
	private final TableMeta index_meta;
	
	public LFSDirStore(String store_file_name, String mode, int bufbitlen) throws IOException {
		// 2^12=4*1024=4K buff size, bigger then faster
		super(store_file_name, mode, bufbitlen);
		
		index_meta = new TableMeta( store_file_name + ".meta");
		if(index_meta.getMaxVal() < 0)
		{
			//index_meta.max_value = 0;
			index_meta.setMaxVal(0);
		}
	}
 
 
	public long length()
	{
		return this.index_meta.getMaxVal();
	}
	
	public void seeEnd() throws IOException
	{
		this.seek(this.index_meta.getMaxVal());
	}
	
	/*
	 * @return the maximum id of this dir file.
	 */
	public long appendDir(int dir_entry_size, long dir_value) throws IOException {
		synchronized (this) {
			growGreedy(index_meta.getMaxVal() , dir_entry_size); 
			seek(index_meta.getMaxVal() );
			long _id = index_meta.getMaxVal()  / dir_entry_size ;
			this.WriteLong(dir_value); 
			
			//index_meta.max_value = index_meta.max_value + dir_entry_size;
			index_meta.setMaxVal(index_meta.getMaxVal() + dir_entry_size);
			//flush();
			return _id;
		}
	} 
	/*
	 * @return: the maximum id of this dir file.
	 */
	public long appendDir(int dir_entry_size, byte dir_value) throws IOException {
		synchronized (this) {
			growGreedy(index_meta.getMaxVal() , dir_entry_size); 
			seek(index_meta.getMaxVal() );
			long _id = index_meta.getMaxVal()  / dir_entry_size ;
			this.write(dir_value); 
			
			//index_meta.max_value = index_meta.max_value + dir_entry_size;
			index_meta.setMaxVal( index_meta.getMaxVal() + dir_entry_size);
			return _id;
		}
	} 
	public void flush() throws IOException 
	{ 
		super.flush(); 
		index_meta.updateMeta(); 
	}
	
	public void close() 
	{ 
		try {
			index_meta.updateMeta();
			index_meta.close(); 
		} catch (IOException e) { 
			e.printStackTrace();
			System.err.println("[ERROR]: fail to close @LFSDirStore.close()");
		}
		finally
		{
			super.close();
		}
		
	}
	
 
	 
	 
	 

}

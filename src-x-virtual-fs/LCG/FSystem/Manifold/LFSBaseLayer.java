/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contactor: 
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
 
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;

import LCG.Concurrent.CacheBig.Controller.impl.BlockCache;
import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.Def.DBFSProperties;
import LCG.StorageEngin.IO.L0.IOInterface;
import LCG.StorageEngin.IO.L1.IOStream;
import LCG.StorageEngin.IO.L1.IOStreamNative;
import LCG.Utility.StrLegitimate;
import LCG.StorageEngin.Serializable.IObject;

//For concurrent random read and write, and the locker is a stream level 
//locker, i.e. one stream access has no effect to others.
//It has no global file pointer to indicate where the 
//current position is inside the file.
//Therefore every function need a parameter to tell where begins 
//reading or writing.

public class LFSBaseLayer {

	static ResourceBundle res = ResourceBundle.getBundle("LCG.StorageEngin.IO.L0.Res");
    
	public List< IOInterface> cardinates; 
	//public IOInterface objects_dir;
	public LFSDirStore objects_dir;
	protected long object_count;
	
	private final String _dir_suffix = "dir";
	protected File dir;
	private String root_path; 
	private String _file_prefix;
	
	protected String base_file_name;
	
	protected final DBFSProperties dbfs_prop_instance;
	protected final int seg_file_length ; 
	protected int max_blocks_in_seg;
	protected int buff_bit_len; 
	protected int buff_size;
	private int g_current_index;
	private int g_current_offset;
	
	private boolean read_only = false; 
	private boolean is_native =  false;
	
    public static LFSBaseLayer getLayerInstance(String _root_path, 
    											String _base_name, 
    											String _mode, 
    											int _bit_buff_len, 
    											boolean _io_mode, 
    											DBFSProperties _dbfs_prop_instance) throws IOException 
    { 
        return new LFSBaseLayer(_root_path, _base_name, _mode, _bit_buff_len, _io_mode, _dbfs_prop_instance); 
    }  
    
    public LFSBaseLayer(String _root_path, 
    					String _base_name, 
    					String _mode, 
    					int _bit_buff_len, 
    					boolean _io_mode,
    					DBFSProperties _dbfs_prop_instance) throws IOException  
    { 
    	if (_mode.equals(res.getString("r")) == true) { 
            this.read_only = true;
        } else { 
            this.read_only = false;
        }
    	
    	dbfs_prop_instance = _dbfs_prop_instance;
    	seg_file_length = _dbfs_prop_instance.seg_file_size; 
    	
    	is_native = _io_mode; 
    	 
    	cardinates = new ArrayList<IOInterface>(); 
    		 
    	this.root_path = StrLegitimate.purifyStringEn(_root_path);
    	if(!this.root_path.endsWith("/"))
    		this.root_path = this.root_path + "/";
    	this._file_prefix = StrLegitimate.purifyStringEn(_base_name);
    	this.base_file_name = this.root_path + _file_prefix + ".";
    	
    	
    	if(_bit_buff_len <0 || _bit_buff_len >30)
    		this.buff_bit_len = _dbfs_prop_instance.bit_buff_len;
    	else
    		this.buff_bit_len = _bit_buff_len;
    	
    	this.buff_size = 1<<this.buff_bit_len;
    	
    	dir = new File(this.root_path);
    	if(!dir.isDirectory())
    		throw new IOException("[ERROR]:the root directory " + root_path + " is wrong, initate with a correct directory");
    	else
		{ 
    		NameFilter _filter = new NameFilter(_file_prefix);
			File[] file_array=dir.listFiles(_filter);
            if(file_array!=null)
            {
            	if(file_array.length == 0)
                {
                	this.spawn();
                	this.createDirectory();
                }
            	else
            	{
                for (int i = 0; i < file_array.length; i++) { 
                	String suffix_i = String.valueOf(i);
                	File file_i = new File(base_file_name+suffix_i);
                	if(file_i.exists()) 
                	{
                		if(is_native) 
                		{
                			IOStreamNative ios_i = new IOStreamNative(base_file_name+suffix_i, "rw", this.buff_bit_len);
                    		this.cardinates.add(ios_i);
                    		System.out.println("Native Cardinate " + i + " has length " + ios_i.length());
                		}
                		else
                		{
                			IOStream ios_i = new IOStream(base_file_name+suffix_i, "rw", this.buff_bit_len);
                			this.cardinates.add(ios_i);
                			System.out.println("Cardinate " + i + " has length " + ios_i.length());
                		}
                	}
                	else
                	{
                		File file_dir = new File(base_file_name + _dir_suffix);
                		if(!file_dir.exists())
                		{
                			System.out.println("Error! Blocks directory file " + base_file_name + _dir_suffix + " is missing. Create one now... ");
                			 
                		}
                		if(is_native) 
                		{
                			//objects_dir = new IOStreamNative(base_file_name + _dir_suffix, "rw", this.buff_bit_len);
                			objects_dir = new LFSDirStore(base_file_name + _dir_suffix, "rw", this.buff_bit_len);
                		}
                		else
                		{	//objects_dir = new IOStream(base_file_name + _dir_suffix, "rw", this.buff_bit_len);
                			objects_dir = new LFSDirStore(base_file_name + _dir_suffix, "rw", this.buff_bit_len);
                		}
                	}
                }
            	} 
            } 
		}
    	
    	
		
    	g_current_index = 0;
    	g_current_offset = 0; 
    }
    
    private void regist(IOInterface new_stream)
    {
    	this.cardinates.add(new_stream);
    }
    
    public IOInterface acquir(int element_at)
    {
    	return this.cardinates.get(element_at);
    }

    private void createDirectory() throws IOException
    {
    	if(acquirDisk())
    	{
    		String suffix = String.valueOf(this.cardinates.size());
    		if(this.isNativeMode())
    		{
    			//objects_dir = new IOStreamNative(base_file_name + _dir_suffix, "rw", this.buff_bit_len);
    			objects_dir = new LFSDirStore(base_file_name + _dir_suffix, "rw", this.buff_bit_len);
    		}
    		else
    		{
    			//objects_dir = new IOStream(base_file_name + _dir_suffix, "rw", this.buff_bit_len);
    			objects_dir = new LFSDirStore(base_file_name + _dir_suffix, "rw", this.buff_bit_len);
    		}
    	}
    	else
    	{
    		throw new IOException("No valid disk space for new file");
    	} 
    }
    
    protected IOInterface spawn() throws IOException
    {
    	if(acquirDisk())
    	{
    		String suffix = String.valueOf(this.cardinates.size());
    		IOInterface new_stream;
    		if(this.isNativeMode())
    			new_stream = new IOStreamNative(base_file_name + suffix, "rw", this.buff_bit_len);
    		else
    			new_stream = new IOStream(base_file_name + suffix, "rw", this.buff_bit_len);
    		
    		this.regist(new_stream);
        	return new_stream;
        		
    	}
    	else
    	{
    		throw new IOException("No valid disk space for new file");
    	} 
    }
    
    public IOInterface getLatest()
    {
    	return this.cardinates.get(this.cardinates.size()-1);
    }

    public int interfaceCount()
    {
    	return this.cardinates.size();
    }
    
    public boolean acquirDisk()
    {
    	return true;
    }
     
 
	public void seek(long pos) throws IOException {
    	g_current_index =(int) pos/this.seg_file_length;
    	g_current_offset = (int) pos % this.seg_file_length;
    	this.cardinates.get(g_current_index).seek(g_current_offset); 
	}
	
	public int getStreamIndex(long pos)
	{
		return (int) pos/this.seg_file_length;
	}

	public int getOffset(long pos)
	{
		return (int) pos % this.seg_file_length;
	}
	
	public int getSegLength()
	{
		return this.seg_file_length;
	}

	public boolean isReadOnly()
	{
		return this.read_only;
	}
	@Deprecated
	public void seekEnd() throws IOException {
		// TODO Auto-generated method stub
		
	} 
	
	protected void appendDir() throws IOException {};
	 
	protected long calcDataLengthInLatestIOStream(){return -1;};
	
	//protected synchronized int appendObjInDisk(T new_obj) 
	protected synchronized long appendObjInDisk(int obj_size) 
	{   
		IOInterface io_latest = this.getLatest(); 
		 
		//long new_length = io_latest.dataLength() + obj_size;
		long new_length = calcDataLengthInLatestIOStream() + obj_size;
		 
		if(new_length<=this.seg_file_length)
		{
			//new_obj.setID(object_count);
			try
			{
				
				/*
				 * grow the file length by appending a new object, 
				 * but not actually write it 
				 */
 
				//io_latest.setDataLength(new_length); 
				io_latest.growGreedy(new_length-obj_size, obj_size);
				
				appendDir();
				object_count++;
				//this.writeObj(new_obj, new_obj.getID());
			 
				//this._mem_pool.returnObj(new_obj.getReference(), this); 
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//new_obj.setID(-1);
				return -1;
			}
			//return new_obj.getID();
			return object_count;
			 
		}
		else if(this.acquirDisk())
		{
			//new_obj.setID(object_count);
			try
			{
				/*
				 * expand the latest seg file to the standard size.
				 */
				//io_latest.setDataLength(this.seg_file_length);
				
				
				
				IOInterface io_new = this.spawn(); 
				synchronized (io_new)
				{ 
					//io_new.setDataLength(obj_size);
					io_new.growGreedy(0, obj_size);
				}
				appendDir();
				object_count++;
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//new_obj.setID(-1);
				return -1;
			}
			//this._mem_pool.returnObj(new_obj.getReference(), this);
			//return new_obj.getID();
			return object_count;
		}
		else
			return -1;
		 
	}  
	
	
	public boolean isNativeMode()
	{
		return this.is_native;
	}
	
	public long length() {
		long l = 0;
		for(int i = 0; i< this.cardinates.size();i++)
		{
			l+=this.cardinates.get(i).length();
		}
		 
		return l;
	} 
	
	//must run before close
	public synchronized void flush() throws IOException
	{
		/*
		 * since the native lib has been changed to be IOSwapDM, 
		 * which has no internal buffer to cache data, 
		 * then here needs no flush of each io file.
		 */
		/*
		Iterator<IOInterface> it = this.cardinates.iterator();
		if (it != null) 
		{
			while (it.hasNext())
			{ 
				it.next().flush();
			}
		}
		*/	
		objects_dir.flush();
	}
	 
	public synchronized void close() throws IOException {
		//flush();
		Iterator<IOInterface> it = this.cardinates.iterator();
		if (it != null) 
		{
			while (it.hasNext())
			{
				it.next().close();
			}
		}
		objects_dir.close();
	}
    
    

}

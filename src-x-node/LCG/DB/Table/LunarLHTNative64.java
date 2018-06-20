/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author LunarBase team 
  * The contents of this file are subject to the Lunarion Public License Version 1.0
  * ("License"); You may not use this file except in compliance with the License
  * The Original Code is:  Lunar NoSQL Database source code 
  * The Lunar NoSQL Database source code is based on Lunarion Cloud Platform(solution.lunarion.com)
  * The Initial Developer of the Original Code is the development team at Lunarion.com.
  * Portions created by lunarion are Copyright (C) lunarion.
  * All Rights Reserved.
  *******************************************************************************
 * 
 */

package LCG.DB.Table;

import java.io.File;
import java.io.IOException;

import LCG.MemoryIndex.DefaultNativePath;

public class LunarLHTNative64 {
    protected long _llht_mem_addr; 
    private boolean closed;
    private boolean terminated;
    private String _name;
    static {
    	try{  
    		String abs_path = new File (".").getCanonicalPath();
    		
    		System.out.println( "Native lib path is: " + abs_path );
    		System.load(abs_path + "/libLinux_X86_64_IOL0_LLHT.so"); 
    	} catch(UnsatisfiedLinkError e) 
    	{ 
            System.err.println("Cannot load Linux_X86_64_IOL0_LLHT library:\n " + 
            		e.toString() ); 
            
            System.out.println("Trying load native lib at: " + DefaultNativePath.final_native_lib_location );
            System.load(DefaultNativePath.final_native_lib_location + "libLinux_X86_64_IOL0_LLHT.so"); 
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }
    

    public LunarLHTNative64(String name, String mode, int hash_bucket_cache_size, int concurrency_level, int bufbitlen) throws IOException 
    { 
    	init(name, mode, hash_bucket_cache_size, concurrency_level, bufbitlen);
    	_name = name;
    	closed = false;
    	terminated = false;
    } 
    protected void terminate() 
    { 
    	if(terminated)
    		return;
    	
        try { 
        	terminator(_llht_mem_addr); 
        	terminated = true;
        } finally { 
            try { 
                super.finalize(); 
            } catch (Throwable e) { 
                // TODO Auto-generated catch block 
                e.printStackTrace(); 
            } 
        } 
    } 
    
    public void close()
    {
    	if(closed)
    		return;
    	
    	closeLLHT(_llht_mem_addr);
    	terminate();
    	closed = true;
    }
    
    public native long initLLHT(String name, String mode, int hash_bucket_cache_size, int concurrency_level ,int bufbitlen);
    public native void closeLLHT(long llht_mem_addr);
    private native void terminator(long llht_mem_addr);

    public native boolean	insertKV(long llht_mem_addr, long key, long val); 
    public native long	totalElement(long llht_mem_addr);
    public native long	searchKey(long llht_mem_addr, long key);  
    public native void	save(long llht_mem_addr );  
    
    /*
     * name: full path
     */
    public void init(String name, String mode, int hash_bucket_cache_size, int concurrency_level ,int bufbitlen) throws IOException {
    	_llht_mem_addr = initLLHT(name, mode, hash_bucket_cache_size, concurrency_level, bufbitlen);
	}

    public boolean insert(long key, long val)
    {
    	return insertKV(_llht_mem_addr, key, val);
    }
    
    public void save()
    {
    	save(_llht_mem_addr);
    }
    
    public long totalElement()
    {
    	return totalElement(_llht_mem_addr);
    }
    
    /*
     * -1 if key is not found, else a long value for the key in llht K-V store.
     */
    public long search(long key)
    {
    	return searchKey(_llht_mem_addr, key); 
    	/*
		long start = System.nanoTime();
		
		long key_id = searchKey(_llht_mem_addr, key);  
		 
		long end = System.nanoTime(); 
		System.out.println("call native costs: " + (end - start) + " ns"); 
		return key_id;
		*/
    }
    
}

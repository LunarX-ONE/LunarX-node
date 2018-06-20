/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author DADA database team 
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import LCG.Concurrent.CacheBig.CacheLRU;
import LCG.FSystem.Def.DBFSProperties;
import LCG.MemoryIndex.IndexTypes.DataTypes;
import LCG.Utility.StrLegitimate;
import LunarX.Memory.RowKeyCache;
import LunarX.Node.Conf.EnginPathDefinition;
import LunarX.Node.Utils.HashCadidates;
import LunarX.RecordTable.StoreUtile.LunarColumn;
import Lunarion.SE.AtomicStructure.RowEntry;
import Lunarion.SE.AtomicStructure.TermScore;
import Lunarion.SE.FullText.Lexer.NoTokenizer;
import Lunarion.SE.FullText.Lexer.SimpleTokenizer;
import Lunarion.SE.FullText.Lexer.TokenizerInterface;
import Lunarion.SE.HashTable.Stores.StoreIDString;
import Lunarion.SE.HashTable.Stores.StoreRowEntries;

public class LunarLHTEngine {
	  
	private LunarLHTNative64 lunar_lht;
	private StoreRowEntries row_entry_store;
	RowKeyCache rk_cache;
	 
	 
	
	/*
	 * by default, it is a SimpleTokenizer, which is not efficient enough.
	 */
	private TokenizerInterface tokenizer = new SimpleTokenizer();

	private final String root_path;
	private final DBFSProperties dbfs_prop_instance;
	
	DataTypes column_type; 
	
	public LunarLHTEngine(String _table_path, 
			String _column_name, 
			DataTypes _column_type, 
			DBFSProperties _dbfs_prop_instance) throws IOException {
		if (!_table_path.endsWith("/"))
			root_path = StrLegitimate.purifyStringEn(_table_path) + "/" + StrLegitimate.purifyStringEn(_column_name) + "/";
		else
			root_path = StrLegitimate.purifyStringEn(_table_path) + StrLegitimate.purifyStringEn(_column_name) + "/";

		dbfs_prop_instance = _dbfs_prop_instance;
		
		File root_dir = new File(root_path);

		if (!root_dir.exists() && !root_dir.isDirectory()) {
			root_dir.mkdir();
		}
		rk_cache = new RowKeyCache(dbfs_prop_instance.max_cache_keys_in_memory,
									dbfs_prop_instance.concurrent_level);
		load(_column_name.toLowerCase(), _column_type);
	}

	private void load(String _column_name, DataTypes _column_type ) throws IOException {
		 
		String id_property_file = root_path + EnginPathDefinition.property_dict;
 
		String filter_store_prefix = root_path + EnginPathDefinition.filter_file;

		 
		//property_dict = new StoreIDString(id_property_file, "rw", 12);

		/*
		 * 12 (thus 4k io cache) is good for totally random hash access to the hard driver.
		 * Bigger bit_buff_len does no good for performance
		 */
		row_entry_store = new StoreRowEntries(root_path, 
							StrLegitimate.purifyStringEn(_column_name), 
							EnginPathDefinition.row_entry_storage, 
							"rw", 
							12,
							dbfs_prop_instance); 
		
		lunar_lht = new LunarLHTNative64(root_path + StrLegitimate.purifyStringEn(_column_name)+".llht", "rw", 
				dbfs_prop_instance.max_cache_keys_in_memory, 
				//256,
				dbfs_prop_instance.concurrent_level, 
				//DBFSProperties.bit_buff_len);
				12);
		
		column_type = _column_type;
		if(column_type == DataTypes.VARCHAR)
		{
			tokenizer = new NoTokenizer(); 
		}
		/*
		int bucket_cache_size = 256;
		int cache_concurrency_level = 18;
		int bit_buff_len = 12;
		 
		lunar_lht = new LunarLHTNative64(root_path + StrLegitimate.purifyStringEn(_db_name)+".llht", "rw", 
				bucket_cache_size, 
				cache_concurrency_level, 
				cache_concurrency_level);
		
		 */
		
	}

	
	public void registerTokenizer(TokenizerInterface __tokenizer)
	{
		if(column_type == DataTypes.VARCHAR)
			return ;
		
		this.tokenizer = __tokenizer;
	}
	
	public TokenizerInterface getTokenizer()
	{
		return this.tokenizer;
	}

	public void index(int rec_id, HashMap<String, TermScore> ter_score_map) throws IOException
	{
		if(rec_id == -1 || ter_score_map == null || lunar_lht == null)
			 return; 
	 		
		Iterator<String> it = ter_score_map.keySet().iterator();
		while(it.hasNext())
		{
			String term = it.next(); 
			//ter_score_map.get(term) ; 
			commitIndex( rec_id, ter_score_map.get(term).getTerm().toLowerCase() );		 
		}
		
		// return true;
		//System.out.println(
		//		"Succeed@ Thread " + Thread.currentThread().getName() + " inserted record " + record + ".");

	}
	 
	public void index(int rec_id, LunarColumn col ) throws IOException
	{
		if(rec_id == -1 || col == null || lunar_lht == null)
			 return;
		 
		 
		String column_content = col.getColumnValue(); 
			
		//long start_time = System.nanoTime();   
		
		HashMap<String, TermScore> ter_score_map  = this.tokenizer.tokenizeTerm(column_content);
		
		//long end_time = System.nanoTime();
		 
		// long duration_each = end_time - start_time;
		 
		// System.err.println("@" + Thread.currentThread() + ",  tokenize has taken: " + duration_each + " ns");  
		
		
		Iterator<String> it = ter_score_map.keySet().iterator();
		while(it.hasNext())
		{
			String term = it.next(); 
			//ter_score_map.get(term) ; 
			commitIndex( rec_id, ter_score_map.get(term).getTerm().toLowerCase() );		 
		}
		
		// return true;
		//System.out.println(
		//		"Succeed@ Thread " + Thread.currentThread().getName() + " inserted record " + record + ".");

	}

	//private void commitIndex(int rec_id, TermScore ts ) throws IOException
	private void commitIndex(int rec_id, String row_key_i ) throws IOException
	{
		RowEntry re_i = null;  
		
		//long start = System.nanoTime();
		re_i = this.readRowEntry(row_key_i, row_key_i);
		//long end = System.nanoTime(); 
		//System.out.println("readRowEntry costs:" + (end-start)+ " ns");
		if (re_i != null) { 
			// start = System.nanoTime();
			re_i.appendDataAsync(rec_id, this.row_entry_store);
			// end = System.nanoTime(); 
			// System.out.println("appendData costs:" + (end-start)+ " ns");
		} 
		else
		{  
			//start = System.nanoTime();
			re_i = 	this.put(row_key_i);
			//end = System.nanoTime(); 
			//System.out.println("put key " + row_key_i + " costs:" + (end-start)+ " ns");
			if(re_i != null)
			{
				//re_i = this.readRowEntry(row_key_i); 
				//if (re_i != null) { 
				// start = System.nanoTime();
				re_i.appendDataAsync(rec_id, this.row_entry_store);
				// end = System.nanoTime(); 
				// System.out.println("appendData costs:" + (end-start)+ " ns");
				//}
			}
			
			else
			{
				System.err.println("[ERROR]: "+row_key_i + " failed to put into the hash table @LunarLHTEngine.commitIndex.");
				return; 
			}			 
		}
		
		if (re_i != null) {
			//re_i.setHandlerLevel(re_i.getVFInst().getHandler().handler_level);
			//re_i.setHandlerID(re_i.getVFInst().getHandler().handler_id);
			/*
			 * 
			 */
		 
			//this.row_entry_store.write(re_i);
			this.rk_cache.addToCache(re_i, row_entry_store);
		}
	}		

	private RowEntry put(String row_key) throws IOException {
 
		long key_hash = HashCadidates.hashJVM(row_key);
		 
		long key_id = lunar_lht.search(key_hash); 
		/*
		 * none existing
		 */
		if (-1 == key_id) {
			/*
			 * it is just the total elements in lunar_lht, 
			 * can not be the key_id, which must be the maximum stored 
			 * row key id in StoreRowEntries
			 */
			//key_id = lunar_lht.totalElement();
			/*
			 * For a new row_key, its id shall get from the 
			 * storage of row entries, but not the hash table(lunar_lht).
			 * 
			 * In case when some exception occur, both hash table 
			 * and the row entry store may 
			 * have no time to flush its cache to disk, 
			 * then the maximum id in both places will not be consistent.
			 * 
			 * Hence here we use what have stored in row entry storage as 
			 * the standard.
			 * 
			 * For the hash storage, its key ids stored may be not continuous, 
			 * but it is not fatal. Since for any row_key, it is guaranteed that 
			 * it has a unique id that can be found in row entry store.
			 */
			RowEntry re = row_entry_store.add(row_key);
			
			/*
			 * if system crash before here, then this row_entry will lost.
			 */
			//row_entry_store.flush();
			rk_cache.addToCache( re, row_entry_store);
			/*
			 * need no comparison here, since once a conflict occurs, 
			 * the totalElement in hash table must less than the 
			 * maximum RowEntry id.
			 */
			/*
			key_id = lunar_lht.totalElement();
			if(key_id !=  re.getKeyID())
			{
				String warning_info = "[WARN]: maximum elements in hash storage "
						+ key_id 
						+" is not equal to the maximum entries it should be: "
						+ re.getKeyID();
				System.err.println(warning_info);
			}
			*/
			/*
			 * if system crashes before here, then all the keys inserted 
			 * to lunar_lht after startup lunarbase may lost.
			 * 
			 * TO DO: Fix it, add a save() function to lunar_lht
			 */
			if(lunar_lht.insert(key_hash, re.getKeyID())) 
			{  
				return re;
			}
			System.err.println("[ERROR]: LunarLHTNative64 can not insert this key = " + row_key
								+ " @LunarLHTEngine.put(...)");
			 
			return null;

		} else {
			/*
			 * key_id is not -1, i.e exists a key, which has the same hash
			 * code as this row_key. check if it is exactly this row_key in the
			 * param list.
			 */
			RowEntry re = row_entry_store.get(key_id); 

			/*
			 * if it is the key, just return null.
			 */
			if (re.s_key_string.Get().equalsIgnoreCase(row_key)) 
			{
				return null;
			}
			else
			{
				/*
				 * another key has the same hash code, a conflict occur.
				 */
				//TODO
				/*
				System.err.println("[WARNING]: " 
									+ re.s_key_string.Get() 
									+ " has the same hash with "
									+ row_key 
									+ ", LunarLHTNative64 can not insert this key = " + row_key
									+ " @LunarLHTEngine.put(...)");
				 */
				String conflicted = row_key.concat(ConflictHandler.conflict_suffix);
				return  putConflict(row_key, conflicted);
			}
		} 
	}

	private RowEntry putConflict(String origin_row_key, String with_suffix) throws IOException
	{
		long key_hash = HashCadidates.hashJVM(with_suffix);
		 
		long key_id = lunar_lht.search(key_hash); 
		/*
		 * none existing
		 */
		if (-1 == key_id) {
			 
			RowEntry re = row_entry_store.add(origin_row_key);
			rk_cache.addToCache( re, row_entry_store);
			 
			if(lunar_lht.insert(key_hash, re.getKeyID())) 
			{  
				return re;
			}
			System.err.println("[ERROR]: LunarLHTNative64 can not insert this key = " + origin_row_key
								+ " @LunarLHTEngine.putConflict(...)");
			 
			return null;

		} else {
			/*
			 * 
			 */
			RowEntry re = row_entry_store.get(key_id); 

			/*
			 * if it is the key, just return null.
			 */
			if (re.s_key_string.Get().equalsIgnoreCase(with_suffix)) 
			{
				return null;
			}
			else
			{
				/*
				 * conflicts again.
				 */
				 
				String conflicted = with_suffix.concat(ConflictHandler.conflict_suffix);
				return  putConflict(origin_row_key, conflicted);
			}
		} 
	}
	
	/*
	 * it is recursively read RowEntry for row_key.
	 * If conflicts, append row_key with a suffix, and call readRowEntry 
	 * again, till find the entry of row_key or returns null 
	 * if it is a new key.
	 */
	public RowEntry readRowEntry(String row_key, String with_suffix_if_conflict) throws IOException {
		 
		
		RowEntry row_entry = null;
		
		 
		//long start = System.nanoTime();  
		row_entry = this.rk_cache.getObj(row_key);
		//long end = System.nanoTime(); 
		//System.out.println("rk_cache.getObj(row_key) costs:" + (end-start)+ " ns"); 
		if(row_entry!=null)
		{ 
			return row_entry; 
		}
			
		
		
		long key_hash = HashCadidates.hashJVM(with_suffix_if_conflict);  
		
		 //long startt = System.nanoTime(); 
		
		/*
		 * if this row_key conflicts a previous key,
		 * then this search will get a key_id
		 */
		long key_id = lunar_lht.search(key_hash); 
	 
		 //long endd = System.nanoTime(); 
		 //System.out.println("LunarLHTEngin call get key id costs: " + (endd - startt ) + " ns"); 
		
		row_entry = null;

		if (-1 == key_id) {
			return row_entry;
		}

		/*
		 * key_id is not -1 (find the key), then check if it is exactly that
		 * record
		 */ 
		 
			 
		  //long starttt = System.nanoTime(); 
		row_entry = row_entry_store.get(key_id);
		
		  //long enddd = System.nanoTime(); 
		  //System.out.println("row_entry_store call get row entry costs: " + (enddd - starttt) + " ns"); 
		
		/*
		 * Some one suggests that we should first check if the two strings
		 * have the same length to avoid the time consuming string
		 * comparison like this: if(kisp.s_key_string.Size() ==
		 * row_key.length())
		 * 
		 * But, conflict is with small probability, and for most of the
		 * keys, if two of them have the same hash code, they are equal.
		 * 
		 * So, it is a waste to compare the length of most of the right keys
		 * for the purpose of eliminating a small account of conflicted
		 * keys.
		 * 
		 * Here we compare the two strings immediately.
		 * 
		 */ 
		if (row_entry.s_key_string.Get().equalsIgnoreCase(row_key)) 
		{
			this.rk_cache.addToCache(row_entry, row_entry_store);
			return row_entry;
		}
			
		else
		{	/*
			 * conflict
			 */
			String conflicted =  with_suffix_if_conflict.concat(ConflictHandler.conflict_suffix);
			return  readRowEntry(row_key,conflicted);
		}
	}

	 

	private void FlushASeg(CacheLRU<String, RowEntry> one_seg) throws IOException
	{
		if(one_seg ==null)
			return;
		if(one_seg.size()==0)
			return;
		 
		one_seg.lockOuter();
		Iterator<Entry<String, RowEntry>> it = (Iterator<Entry<String, RowEntry>>)one_seg.KeyValueIterator();
		if (it != null) 
		{
			while (it.hasNext()) 
			{  
				java.util.Map.Entry<String, RowEntry> e = it.next();
		        if (e != null && e.getValue() != null && e.getValue() != null) 
		        { 
		        	RowEntry t_obj = e.getValue();  
		        	if(t_obj.isDirt())
		        	{ 
		        		this.row_entry_store.write(t_obj);
		        	} 
		        	//System.out.println("flush this cached block to disk : " + t_obj.s_obj_id);  
		        }
		    }
		}
		
		one_seg.unlockOuter();
		 
	}
	
	//just flush, do not clear the cache. 
	//Actually, has no privilege to clear a global cache.  
 
	public void flushEntries() throws IOException { 
		
		//flush all the cache to disk 
		
		/*
		int con_level = this.rk_cache.getConcurrencyLevel();
		for(int i =0; i< con_level; i++)
		{
			CacheLRU<String, RowEntry> cache_seg = this.rk_cache.getCacheCongruent(i);

			FlushASeg(cache_seg); 
					 
		} 
		*/
		
		/*
		 * just do not flush these dirty entries, since it is slow for flushing 
		 * all entries with only tens of ids belonging to each of it.
		 * 
		 * If system crashes, let the record ids belonging to these blocks lost.
		 */
		//this.rk_cache.flushDirtyEntries(row_entry_store);
		row_entry_store.flushVFSBaseIO(); 
		row_entry_store.flush(); 
		this.lunar_lht.save();
		//property_dict.flush(); 
	 	
		 
	}

	public void flush() throws IOException { 
		this.rk_cache.flushDirtyEntries(row_entry_store);
		row_entry_store.flushVFSCache();
		row_entry_store.flushVFSBaseIO(); 
		row_entry_store.flush();  
		this.lunar_lht.save();
	}
	public void close() throws IOException {  

		row_entry_store.flushVFSCache();
		row_entry_store.flushVFSBaseIO(); 
		row_entry_store.flush(); 
		row_entry_store.close();

		// id_rowkey_store.flush();
		// id_rowkey_store.close();

		//property_dict.flush();
		//property_dict.close();
		
		rk_cache.shutDown();
		lunar_lht.close();
	}
}

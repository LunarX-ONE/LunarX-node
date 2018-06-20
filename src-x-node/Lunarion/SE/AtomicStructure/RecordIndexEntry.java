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

package Lunarion.SE.AtomicStructure;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import LCG.StorageEngin.IO.L0.IOInterface;
import LunarX.RecordTable.RecStatusUtile;
import LunarX.RecordTable.RecStatusUtile.RecStatus;

public class RecordIndexEntry {
	//public int rec_id;
	public int s_rec_status;
	public RecStatus rec_status;
	public long s_position;
	
	/*
	 * hidden variables for transaction. 
	 */
	private AtomicInteger s_version = new AtomicInteger();  
	

	public RecordIndexEntry() {
		//rec_id = -1;
		s_rec_status = -1;
		s_position = -1L;
		s_version.set(0);
	}

	//public RecordIndexEntry(int _rec_id, long _pos) {
	public RecordIndexEntry(long _pos) {
		//rec_id = _rec_id;
		s_position = _pos;
		s_version.set(0);
	}
	
	private RecordIndexEntry(AtomicInteger new_version, long _pos) 
	{
		s_position = _pos;
		s_version.set(new_version.get());
	}
	
	//public RecordIndexEntry newVersion(long _new_pos)
	//{
	//	s_version.incrementAndGet();
	//	return new RecordIndexEntry(s_version, _new_pos);
	//}
	
	//public void setStatus(RecStatus rs)
	//{
	//	this.rec_status = rs;
	//}
	public void insert()
	{
		this.rec_status = RecStatus.inserted;
	}
	public void index()
	{
		this.rec_status = RecStatus.indexed;
	}
	public void delete()
	{
		this.rec_status = RecStatus.deleted;
	}
	
	public void cmdSucceed()
	{
		this.rec_status = RecStatus.succeed;
	}
	
	public void cmdFailed()
	{
		this.rec_status = RecStatus.failed;
	}
	
	public long getPosition()
	{
		return this.s_position;
	}
	public void update(long _new_pos, int new_version)
	{
		this.rec_status = RecStatus.updated;
		s_version.set(new_version);
		this.s_position = _new_pos;
	}
	
	public AtomicInteger getLatestVersion()
	{
		return this.s_version;
	}
	public boolean isDeleted()
	{
		return (this.rec_status == RecStatus.deleted)? true:false;
	}
	
	public boolean isUpdated()
	{
		return (this.rec_status == RecStatus.updated)? true:false;
	} 
	public boolean isSucceed()
	{
		return (this.rec_status == RecStatus.succeed)? true:false;
	} 
	public boolean isFailed()
	{
		return (this.rec_status == RecStatus.failed)? true:false;
	} 

	public void Read(IOInterface io_v) throws IOException {
		synchronized(this)
		{  
			s_rec_status = io_v.read4ByteInt();
			if (s_rec_status != -1)
			{
				rec_status = RecStatusUtile.getStatus((byte)s_rec_status);
				s_position = io_v.ReadLong();
				
				s_version.set(io_v.read4ByteInt());
				 
			}
			else
				s_position = -1L;
		}
	}

	public void Write(IOInterface io_v) throws IOException {
		synchronized(this)
		{ 
			s_rec_status = rec_status.getByte();
			if (s_rec_status != -1 && s_position != -1) {
				io_v.write4ByteInt(s_rec_status);
				io_v.WriteLong(s_position);
				
				io_v.write4ByteInt(s_version.get());  
			}
		}
	}

	static public int SizeInByte() {
		return 12 + 4;//  
	}
}

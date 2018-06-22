package LCG.FSystem.AtomicStructure;

import java.io.IOException;

import LCG.StorageEngin.IO.L1.IOStream;

public class FragmentHead { 
	public long s_version = 1L;
	public int s_total_vblocks;
	//frags are those tails which once have data, but removed to other places
	public int s_total_frags;
	public long s_total_frag_size; 
		
	public FragmentHead()
	{ 
		s_total_vblocks = 0;
		s_total_frags = 0;
		s_total_frag_size = 0;
	}
		
	public int Size()
	{
		return 8+4+4+8;
	}
		
	public void Write(IOStream io_v) throws IOException
	{
		synchronized(io_v)
		{
			io_v.WriteLong(this.s_version);
			io_v.write4ByteInt(this.s_total_vblocks);
			io_v.write4ByteInt(this.s_total_frags);
			io_v.WriteLong(this.s_total_frag_size);
		}
	}
		
	public void Read(IOStream io_v) throws IOException
	{
		synchronized(io_v)
		{
			this.s_version = io_v.ReadLong();
			this.s_total_vblocks = io_v.read4ByteInt();
			this.s_total_frags = io_v.read4ByteInt();
			this.s_total_frag_size = io_v.ReadLong();
		}
	}
}
	

 

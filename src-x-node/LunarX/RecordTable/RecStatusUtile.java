package LunarX.RecordTable;

public class RecStatusUtile {
	
	public enum RecStatus
	{ 
		inserted 
			{ 
				public byte getByte()
				{
					return 0;
				}
			}, 
		deleted 
			{ 
				public byte getByte()
				{
					return 1;
				}
			}, 
		updated 
			{ 
				public byte getByte()
				{
					return 2;
				}
			}, 
		indexed 
			{ 
				public byte getByte()
				{
					return 3;
				}
			},
	    failed 
			{ 
				public byte getByte()
				{
					return 4;
				}
			},
	    succeed 
			{ 
				public byte getByte()
				{
					return 5;
				}
			}, 
		unknown 
		{ 
			public byte getByte()
			{
				return 6;
			}
		}; 
		 
		public abstract byte getByte();
	}; 
	

	public static RecStatus getStatus(byte b)
	{
		switch(b)
		{
		case 0:
			return RecStatus.inserted;
		case 1:
			return RecStatus.deleted;
		case 2:
			return RecStatus.updated;
		case 3:
			return RecStatus.indexed;
		case 4:
			return RecStatus.failed;
		case 5:
			return RecStatus.succeed;
		default:
			return RecStatus.unknown;
		}
	}
	
}

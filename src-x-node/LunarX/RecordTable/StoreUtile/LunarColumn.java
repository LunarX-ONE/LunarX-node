package LunarX.RecordTable.StoreUtile;

public class LunarColumn {
	private String[] column;
	private int begin_at;
	private int end_at;
	
	public LunarColumn()
	{
		begin_at = -1;
		end_at = -1;
		column = new String[2];
	}
	public LunarColumn(String[] col_val)
	{
		column = new String[2];
		column[0] = col_val[0];
		column[1] = col_val[1];
	}
	
	public LunarColumn(String col, String val)
	{
		column = new String[2];
		column[0] = col ;
		column[1] = val ;
	}
	
	public void setColumnName(String column_name)
	{
		column[0] = column_name;
		
	}
	
	public void setColumnValue(String column_value)
	{
		column[1] = column_value;
		
	}
	public String getColumnName()
	{
		return column[0];
	}
	
	public String getColumnValue()
	{
		return column[1];
	}
	
	public void setBeginAt(int at)
	{
		this.begin_at = at;
	}
	
	public void setEndAt(int at)
	{
		this.end_at = at;
	}

	public int getBeginAt()
	{
		return this.begin_at;
	}
	
	public int getEndAt()
	{
		return this.end_at;
	}
}

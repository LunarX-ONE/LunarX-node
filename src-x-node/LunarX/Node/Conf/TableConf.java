package LunarX.Node.Conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import LCG.MemoryIndex.IndexTypes;
import LCG.MemoryIndex.IndexTypes.DataTypes;
import LunarX.RecordTable.StoreUtile.Record32KBytes;

public class TableConf {
	
	String table_conf; 
	
	Properties table_property = null;
	FileInputStream   pInStream = null; 
	FileOutputStream   pOutStream = null; 
	public class ColumnClassified{
		final static String rt_searchable = "rt_searchable";
		final static String rt_storable = "rt_storable";
		final static String fulltext_searchable = "fulltext_searchable";
	}
	class PersistentStatus{
		final static String removed = "removed";
		final static String yes = "yes"; 
		final static String no = "no"; 
	}
	 
	boolean table_removed = false;
	
	public DataTypes[] rt_searchable_data_types = null;
	public String[] rt_searchable_columns = null; 
	public String[] rt_stored_columns = null;
	public String[] fulltext_searchable_columns = null;
	
	/*
	 * although we know what the table is here, we still use this array, 
	 * in the case LunarTable is used as a materialized view whose columns 
	 * coming from variant other tables.
	 */
	public String[] rt_searchable_tables = null;
	public String[] rt_stored_tables = null; 
	public String[] fulltext_searchable_tables = null; 
	
	public TableConf(String conf_file)
	{
		table_conf = conf_file;
	}
	
	public void close() throws IOException
	{
		if(pInStream!=null)
		{
			pInStream.close();
			pInStream = null;
		}
		if(pOutStream != null)
		{	
			pOutStream.close(); 
			pOutStream = null;
		}
	}
	public void loadConfig()
	{
		File p_conf_file = new File(table_conf);   

		try { 
			if(pInStream != null)
				pInStream.close();
			
			pInStream = new FileInputStream(p_conf_file );
		} 
		catch ( IOException e) 
		{
			e.printStackTrace();
		}
		table_property = new Properties();
		try {
			table_property.load(pInStream );  
		} catch (IOException e) 
		{
			e.printStackTrace(); 
		}
		Enumeration enu = table_property.propertyNames();  
		
		while( enu.hasMoreElements())
		{
			//String key = StrLegitimate.purifyStringEn((String)enu.nextElement()).trim();
			//String value = StrLegitimate.purifyStringEn(p.getProperty(key)).trim();
			String key = ((String)enu.nextElement()).trim();
			String value = table_property.getProperty(key).trim();
			String[] tp = value.split(",");
			switch(key)
			{ 
			case PersistentStatus.removed:
				if(tp[0].equalsIgnoreCase(PersistentStatus.yes)) 
					this.table_removed = true;
				else
					this.table_removed = false;
				break;
				
			case ColumnClassified.rt_searchable: 
				
				rt_searchable_data_types = new DataTypes[tp.length];
				rt_searchable_columns = new String[tp.length];
				rt_searchable_tables = new String[tp.length];
				for(int i=0; i<tp.length; i++)
				{ 
					String[] comma_split = tp[i].trim().split(":");
					rt_searchable_data_types[i] = IndexTypes.getDatatype(comma_split[0].trim().toLowerCase());
						
						/*
						switch (comma_split[0].trim().toLowerCase())
						{
							case "int":
								rt_searchable_data_types[i] = DataTypes.INTEGER;
								break;
							case "string":
								rt_searchable_data_types[i] = DataTypes.STRING;
								break;
							case "float":
								rt_searchable_data_types[i] = DataTypes.FLOAT;
								break;
							case "long":
								rt_searchable_data_types[i] = DataTypes.LONG;
								break;
						}
						*/
						String[]  temp = tp[i].trim().split(":")[1].split("\\.");

						rt_searchable_tables[i] = temp[0].trim();  
						rt_searchable_columns[i] = temp[1].trim();  
						 
				 
					
				}
									
				break;
			case ColumnClassified.rt_storable: 
				  
				rt_stored_columns = new String[tp.length];
				rt_stored_tables = new String[tp.length];
				for(int i=0; i<tp.length; i++)
				{  			
					String[] temp = tp[i].trim().split("\\.");
					 
						rt_stored_tables[i] = temp[0].trim();    
						rt_stored_columns[i] = temp[1].trim();  
					 
					
				}
									
				break;
			case ColumnClassified.fulltext_searchable: 
				 
				fulltext_searchable_columns = new String[tp.length];
				fulltext_searchable_tables = new String[tp.length];
				for(int i=0; i<tp.length; i++)
				{  		
					String[] temp = tp[i].trim().split("\\.");
					 
						fulltext_searchable_tables[i] = temp[0].trim();  
						fulltext_searchable_columns[i] = temp[1].trim();  
					 
				}
									
				break;
			}
		}
	}
	
	public boolean addSearchable(String data_type, String table, String column) throws IOException
	{
		return addColumnFor(ColumnClassified.rt_searchable, data_type, table, column) ; 
		
	}
	
	public boolean addStorable(String table, String column) throws IOException
	{ 
		return addColumnFor(ColumnClassified.rt_storable, null, table, column) ; 
	} 

	public boolean addFulltextSearchable( String table, String column) throws IOException
	{
		return addColumnFor(ColumnClassified.fulltext_searchable, null, table, column) ; 
	}

	public boolean setTableRemoved( String table) throws IOException
	{
		return addColumnFor(PersistentStatus.removed, PersistentStatus.yes, table ,null);
	}
	
	public boolean setTableRestored( String table) throws IOException
	{
		return addColumnFor(PersistentStatus.removed, PersistentStatus.no, table ,null);
	}
	
	public boolean isSetRemoved()
	{
		if(this.table_removed)
			return true;
		else
			return false;
	}
	
	private boolean addColumnFor(String for_what, String data_type_or_status, String table, String column) throws IOException
	{
		String tc = null;
		switch(for_what)
		{ 
			case PersistentStatus.removed: 
				tc =  data_type_or_status;
				break; 
			case ColumnClassified.rt_searchable: 
				tc = data_type_or_status +":"+table.trim()+"."+column.trim(); 
				break;
			case ColumnClassified.rt_storable:
				tc = table.trim()+"."+column.trim();
				break;
			case ColumnClassified.fulltext_searchable:
				tc = table.trim()+"."+column.trim();
				break;
		}
		
		if(table_property == null)
			loadConfig();
		
		
		
		Enumeration enu = table_property.propertyNames();  
		
		String affected_prop = null;
		String affected_tc = null; 
		while( enu.hasMoreElements())
		{
			//String key = StrLegitimate.purifyStringEn((String)enu.nextElement()).trim();
			//String value = StrLegitimate.purifyStringEn(p.getProperty(key)).trim();
			String key = ((String)enu.nextElement()).trim();
			String value = table_property.getProperty(key).trim();
			
			switch(key)
			{
			case PersistentStatus.removed: 
				if(for_what.equals(PersistentStatus.removed)) 
				{
					tc = data_type_or_status;
				}
				break;
			case ColumnClassified.rt_searchable:
				if(for_what.equals(ColumnClassified.rt_searchable))  
				{
					tc = data_type_or_status +":"+table.trim()+"."+column.trim();
					for(int i=0;i<rt_searchable_columns.length;i++)
					{
						if(rt_searchable_columns[i].equalsIgnoreCase(column))
							return false;  
					}
					/*
					 * if already in storable columns, remove it from 
					 */
					for(int i=0;(rt_stored_columns!=null && i<rt_stored_columns.length);i++)
					{
						if(rt_stored_columns[i].equalsIgnoreCase(column)
								&& rt_stored_tables[i].equalsIgnoreCase(table))
						{
							affected_prop = ColumnClassified.rt_storable; 
						}
						else
						{
							if(affected_tc!=null)
								affected_tc = rt_stored_tables[i]+"."+rt_stored_columns[i]+","+ affected_tc;
							else
								affected_tc = rt_stored_tables[i]+"."+rt_stored_columns[i];
						}
					}
					if(for_what.equals(ColumnClassified.rt_searchable))
							tc = value+","+ tc;
				}
				if(for_what.equals(ColumnClassified.rt_storable))
				{
					/*
					 * if already in searchable column, just return.
					 */
					for(int i=0;i<rt_searchable_columns.length;i++)
					{
						if(rt_searchable_columns[i].equalsIgnoreCase(column))
							return false;  
					}
				}
				break; 
				
			case ColumnClassified.rt_storable:
				if(for_what.equals(ColumnClassified.rt_storable) ) 
				{
					tc = table.trim()+"."+column.trim();
					
					for(int i=0;i<rt_stored_columns.length;i++)
					{
						if(rt_stored_columns[i].equalsIgnoreCase(column))
							return false;
					}
					/*
					 * if already in searchable column, then do not add it to the storable.
					 */
					for(int i=0;(rt_searchable_columns != null && i<rt_searchable_columns.length);i++)
					{
						if(rt_searchable_columns[i].equalsIgnoreCase(column)
								&& rt_searchable_tables[i].equalsIgnoreCase(table))
							return false;
					}
					tc = value+","+ tc;
				}
				if(for_what.equals(ColumnClassified.rt_searchable))
				{
					/*
					 * if already in storable columns, remove it from 
					 */
					for(int i=0;(rt_stored_columns!=null && i<rt_stored_columns.length);i++)
					{
						if(rt_stored_columns[i].equalsIgnoreCase(column)
								&& rt_stored_tables[i].equalsIgnoreCase(table))
						{
							affected_prop = ColumnClassified.rt_storable; 
						}
						else
						{
							if(affected_tc!=null)
								affected_tc = rt_stored_tables[i]+"."+rt_stored_columns[i]+","+ affected_tc;
							else
								affected_tc = rt_stored_tables[i]+"."+rt_stored_columns[i];
						}
					}
				}
				break; 
				
			case ColumnClassified.fulltext_searchable:
				if(for_what.equals(ColumnClassified.fulltext_searchable))
				{
					tc = table.trim()+"."+column.trim();
					
					for(int i=0;i<fulltext_searchable_columns.length;i++)
					{
						if(fulltext_searchable_columns[i].equalsIgnoreCase(column))
							return false;
					}
					tc = value+","+ tc;
				}
				break; 
			}
		}
			
		 
		File p_conf_file = new File(table_conf); 
		if(pOutStream != null)
			pOutStream.close();
		
		pOutStream = new FileOutputStream(p_conf_file );
		
		//table_property.setProperty(ColumnClassified.fulltext_searchable, tc);
		table_property.setProperty(for_what, tc);
		if(affected_prop != null)
		{
			if(affected_tc!=null)
			{
				table_property.setProperty(affected_prop, affected_tc);
			}
			else
				table_property.remove(affected_prop);
			
		}
		try {
			switch(for_what) 
			{ 
				case PersistentStatus.removed: 
					table_property.store(pOutStream, "closed states the table status, if it is loaded when open or just ignored.");
					
					break;
				case ColumnClassified.rt_searchable:
					table_property.store(pOutStream, "rt_searchable lists all the columns used in real time column query.");
					break;
				case ColumnClassified.rt_storable:
					table_property.store(pOutStream, "rt_storable lists all the columns stored in data view only.");
					break;
				case ColumnClassified.fulltext_searchable:
					table_property.store(pOutStream, "fulltext_searchable lists all the stored columns used in full text search.");
					break;
			}
			pOutStream.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}  
		
		loadConfig(); 
 
		return true;
	}
	public boolean containStorable(String column)
	{
		for(int i=0;i<rt_stored_columns.length;i++)
		{
			if(rt_stored_columns[i].equals(column))
				return true;
		}
		return false;
	}
	
	public boolean containFulltextSearchable(String column)
	{ 
		for(int i=0;i<fulltext_searchable_columns.length;i++)
		{
			if(fulltext_searchable_columns[i].equals(column))
				return true;
		}
		return false;
	}
	
	public static void test1() throws IOException
	{
		String conf_file = "/home/feiben/DBTest/test_table_config.txt";
		File conf = new File(conf_file);
		try {
			conf.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} 
		
		TableConf t_con = new TableConf(conf_file);
		 
		// t_con.setTableRemoved("table1");
		//t_con.setTableRestored("table1");
		System.out.println(t_con.table_removed);
		
		t_con.addStorable( "table1", "column1");
		t_con.addStorable( "table1", "column2");
		/*
		 * can not be added since it is already a searchable column
		 */
		//t_con.addStorable( "table1", "column1");
		//t_con.addStorable( "table1", "column2");
		//t_con.addStorable( "table1", "column3");
		//t_con.addStorable( "table1", "column4");
		
		t_con.addSearchable("string", "table1", "column1");
		t_con.addSearchable("string", "table1", "column2");
		t_con.addSearchable("int", "table1", "column3");
		t_con.addSearchable("int", "table1", "column4");
		
		
		t_con.addFulltextSearchable("table1", "column6");
		t_con.addFulltextSearchable("table2", "column2");
		t_con.addFulltextSearchable("table2", "column3");
		
		/*
		 * test remove column from storable
		 */
		 t_con.addSearchable("int", "table1", "column5");
		
		t_con.close();
		
	}
	
	public static void test2() throws IOException
	{
		String conf_file = "/home/feiben/DBTest/test_table_config2.txt";
		File conf = new File(conf_file);
		try {
			conf.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} 
		
		TableConf t_con = new TableConf(conf_file);
		 
		 
		t_con.addSearchable("string", "table1", "column1");
		 
		
		t_con.addStorable( "table1", "column0");
		t_con.addStorable( "table1", "column1");
		/*
		 * can not be added since it is already a searchable column
		 */
		//t_con.addStorable( "table1", "column1");
		//t_con.addStorable( "table1", "column2");
		//t_con.addStorable( "table1", "column3");
		//t_con.addStorable( "table1", "column4");
		
		
		
		t_con.addFulltextSearchable("table1", "column6");
		t_con.addFulltextSearchable("table2", "column2");
		t_con.addFulltextSearchable("table2", "column3");
		
	 
		t_con.close();
		
	}
	public static void main(String[] args) throws IOException {
		//TableConf.test1();
		TableConf.test2();
	}
}
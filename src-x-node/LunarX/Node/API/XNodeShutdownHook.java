package LunarX.Node.API;

import java.io.IOException;

import LunarX.Node.API.XNodeStatus.DBRuntimeStatus;

public class XNodeShutdownHook extends Thread{

	final LunarXNode _active_db_instance;
	
	XNodeShutdownHook(LunarXNode db_inst)
	{
		this._active_db_instance = db_inst;
	} 
	 
	public void run(){ 
		 if(this._active_db_instance != null)
		 {
			 if(this._active_db_instance.getStatus() != DBRuntimeStatus.closed
					/* && this._active_db_instance.getStatus() != Status.onClosing*/)
			 {
				 System.err.println("Database has been abnormally closing.");
				 System.err.println("Now trying to save data and close db");
				
				 this._active_db_instance.save();
				 try {
					 this._active_db_instance.closeDB();
				 } catch (IOException e) {
					 // TODO Auto-generated catch block
					 e.printStackTrace();
				 }
			 
				 System.err.println("Closed correctly!");
			 }
				
		 }
	 }  

	 
}

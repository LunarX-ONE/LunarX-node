package LunarX.Node.API;

public class XNodeStatus { 
	public enum DBRuntimeStatus
	{ 
		onWaiting,
		onCreating,
		onClosing,
		onOpening,
		closed,
		removed,/*
					a table has been removed, 
					then its data will not be loaded, when database is opening. 
					Hence no data can be inserted into or retrieved from it.
					*/
		onStarting, 
		onReading,
		onWriting,
		onDeleting,
		onSettingStatus,
		onUpdating,  
		onRecovery,  
	};  
}

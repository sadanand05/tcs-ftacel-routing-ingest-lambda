package com.foxtel.ingest.vo;

import java.util.HashMap;
import java.util.Map;


public class RecordEntityVO {
	
	Map<String,String> records = null;
	
	public RecordEntityVO(){
		this.records = new HashMap<String,String>();
	}

	public Map<String, String> getRecords() {
		return records;
	}

	public void setRecords(Map<String, String> records) {
		this.records = records;
	}
	
	
	

}

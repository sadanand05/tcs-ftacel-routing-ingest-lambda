package com.foxtel.ingest.constant;

import java.util.ArrayList;

public interface IngestIO {
	
	public final String VERSION = "20210616-001 initial version";
	
	// CSV file column Index Position
	public final int INDEX_PHONENUMBER1 = 1;
	public final int INDEX_PHONENUMBER2 = 2;
	public final int INDEX_ACCOUNTID = 3;
	
	
	// Customer_DDB Field Name
	public final String COLUMN_ACCOUNTID = "AccountId";
	public final String COLUMN_PHONENUMBER1 = "PhoneNumber1";
	public final String COLUMN_PHONENUMBER2 = "PhoneNumber2";
	public final String COLUMN_ATTRIBUTES = "Attributes";
	
	// Ingest_DDB Field Name
	public final String COLUMN_INGEST_ID = "IngestId";
	public final String COLUMN_FILE_NAME = "FileName";
	public final String COLUMN_INSERT_COUNT = "InsertCount";
	public final String COLUMN_NO_CHANGE = "NoChange";
	public final String COLUMN_RECORD_COUNT = "RecordCount";
	public final String COLUMN_UPDATE_COUNT = "UpdateCount";
	public final String COLUMN_CREATE_DATE = "CreateDate";
	public final String COLUMN_UPDATE_DATE = "UpdateDate";
	public final String COLUMN_STATUS= "Status";
	public final ArrayList<String> COLUMN_AUDIT_INGEST_TABLE = new ArrayList<String>() 
			{{
			    add(COLUMN_INGEST_ID);
			    add(COLUMN_FILE_NAME);
			    add(COLUMN_INSERT_COUNT);
			    add(COLUMN_NO_CHANGE);
			    add(COLUMN_RECORD_COUNT);
			    add(COLUMN_UPDATE_COUNT);
			    add(COLUMN_STATUS);
			    add(COLUMN_CREATE_DATE);
			    add(COLUMN_UPDATE_DATE);
			}};
	
	public enum FILE_AUDIT_STATUS 
	{
		READY,
		RUNNING,
		ERROR,
		PROCESSED,
		UNPROCESSED,
		COMPLETED,
		CURRENT,
		ARCHIVE,
	}
	
	public enum COMPARE_STATUS {
			INSERT,
			UPDATE,
			NOACTION
	};
	
	public final String VALUE_HYPHEN = "-";
	
	public final String ENV_VARIABLE_REGION = "REGION";
	public final String ENV_VARIABLE_CUSTOMER_TABLE_NAME = "CUSTOMER_TABLE_NAME";
	public final String ENV_VARIABLE_INGEST_TABLE_NAME = "INGEST_TABLE_NAME";
	public final String ENV_VARIABLE_THREAD_COUNT = "THREAD_COUNT";
	public final String ENV_VARIABLE_KEY_SECRETS_MANAGER_KEY_ARN = "SECRETS_MANAGER_KEY_ARN";
	public final String ENV_VARIABLE_KEY_SECRETS_MANAGER_PASSPHRASE_ARN = "SECRETS_MANAGER_PASSPHRASE_ARN";
	

}

package jp.co.apasys.create;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

import jp.co.apasys.utils.DBUtils;
import jp.co.apasys.utils.LoggerUtil;

public class AutoCreateThread extends Thread {

    private static String outputDir = "./tables/";

    private String tableName = "";
    private LoggerUtil loggerUtil = null;

    public LoggerUtil getLoggerUtil() {
	return loggerUtil;
    }

    public void setLoggerUtil(LoggerUtil loggerUtil) {
	this.loggerUtil = loggerUtil;
    }

    public AutoCreateThread(String tableName) {
	this.tableName = tableName;
    }

    public void run() {
	try {
	    create();
	} catch (Exception e) {
	    loggerUtil.error(this.tableName, e);
	    e.printStackTrace();
	}
    }

    private void create() throws SQLException, IOException,
	    ClassNotFoundException {
	ResultSet newRs = null;
	ResultSet oldRs = null;

	StringBuilder newKeys = new StringBuilder();
	StringBuilder oldKeys = new StringBuilder();
	StringBuilder newColumns = new StringBuilder();
	StringBuilder oldColumns = new StringBuilder();

	DBUtils dbUtils = new DBUtils();
	dbUtils.initDBConn();

	// get table columns from new db 
	newRs = dbUtils.getNewMetadata().getColumns(null, dbUtils.getNewCreateSchemaName(), this.tableName, null);
	if (newRs != null) {
	    while (newRs.next()) {
		newColumns.append(",");
		newColumns.append(newRs.getString("COLUMN_NAME"));
	    }
	    newRs.close();
	    newRs = null;
	}
	
	// get table columns from old db 
	oldRs = dbUtils.getOldMetadata().getColumns(null, dbUtils.getOldCreateSchemaName(), this.tableName, null);
	if (oldRs != null) {
	    while (oldRs.next()) {
		oldColumns.append(",");
		oldColumns.append(oldRs.getString("COLUMN_NAME"));
	    }
	    oldRs.close();
	    oldRs = null;
	}
	
	// get table keys from new db 
	newRs = dbUtils.getNewMetadata().getPrimaryKeys(null, null, this.tableName);
	if (newRs != null) {
	    while (newRs.next()) {
		newKeys.append(",");
		newKeys.append(newRs.getString("COLUMN_NAME"));
	    }
	    newRs.close();
	    newRs = null;
	}
	
	// get table keys from old db 
	oldRs = dbUtils.getOldMetadata().getPrimaryKeys(null, null, this.tableName);
	if (oldRs != null) {
	    while (oldRs.next()) {
		oldKeys.append(",");
		oldKeys.append(oldRs.getString("COLUMN_NAME"));
	    }
	    oldRs.close();
	    oldRs = null;
	}
	
	// output table properties
	outputTableProperties(tableName, 
		new String(newColumns).replaceFirst(",", ""), 
		new String(newKeys).replaceFirst(",", ""), 
		new String(oldColumns).replaceFirst(",", ""), 
		new String(oldKeys).replaceFirst(",", ""));

    }

    private void outputTableProperties(String tableName, String newColumns,
	    String newKeys, String oldColumns, String oldKeys)
	    throws IOException {
	StringBuilder row = new StringBuilder();
	BufferedOutputStream outStream = new BufferedOutputStream(
		new FileOutputStream(outputDir + tableName + ".properties"));

	row.append("# merge info of new table ");
	row.append(System.getProperty("line.separator"));
	row.append("new.table.name=");
	row.append(System.getProperty("line.separator"));
	row.append(tableName);
	row.append(System.getProperty("line.separator"));
	row.append("new.table.key.column=");
	row.append(System.getProperty("line.separator"));
	row.append(newKeys);
	row.append("new.table.all.column=");
	row.append(System.getProperty("line.separator"));
	row.append(newColumns);
	row.append(System.getProperty("line.separator"));
	row.append("new.table.merge.column=");
	row.append(System.getProperty("line.separator"));
	row.append(newColumns);
	row.append(System.getProperty("line.separator"));
	row.append("new.table.merge.filter=");
	row.append(System.getProperty("line.separator"));
	row.append(System.getProperty("line.separator"));
	row.append(System.getProperty("line.separator"));
	row.append("# merge info of old table ");
	row.append(System.getProperty("line.separator"));
	row.append("old.table.name=");
	row.append(System.getProperty("line.separator"));
	row.append(tableName);
	row.append(System.getProperty("line.separator"));
	row.append("old.table.key.column=");
	row.append(System.getProperty("line.separator"));
	row.append(oldKeys);
	row.append("old.table.all.column=");
	row.append(System.getProperty("line.separator"));
	row.append(oldColumns);
	row.append(System.getProperty("line.separator"));
	row.append("old.table.merge.column=");
	row.append(System.getProperty("line.separator"));
	row.append(oldColumns);
	row.append(System.getProperty("line.separator"));
	row.append("old.table.merge.filter=");

	outStream.write(new String(row).getBytes(StandardCharsets.UTF_8));
	outStream.flush();
	outStream.close();
    }

}

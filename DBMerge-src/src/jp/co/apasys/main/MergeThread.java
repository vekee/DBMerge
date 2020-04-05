package jp.co.apasys.main;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

import jp.co.apasys.model.MergeTableInfo;
import jp.co.apasys.table.MergeSQL;
import jp.co.apasys.table.MergeTable;
import jp.co.apasys.utils.DBUtils;
import jp.co.apasys.utils.LoggerUtil;

public class MergeThread extends Thread {

    private final static Object lock = new Object();

    private Integer maxBucket = 10;
    private Integer hashValue = 1;
    private MergeTableInfo mergeTableInfo = null;
    private LoggerUtil loggerUtil = null;
    private BufferedOutputStream newOnlyOutBuffer = null;
    private BufferedOutputStream oldOnlyOutBuffer = null;
    private BufferedOutputStream diffColumnOutBuffer = null;

    public void setMaxBucket(Integer maxBucket) {
	this.maxBucket = maxBucket;
    }

    public void setHashValue(Integer hashValue) {
	this.hashValue = hashValue;
    }

    public void setMergeTableInfo(String mergeTableFile) throws FileNotFoundException, IOException {
	this.mergeTableInfo = MergeTable.getMergeTableInfo(mergeTableFile);
    }

    public void setLoggerUtil(LoggerUtil loggerUtil) {
	this.loggerUtil = loggerUtil;
    }

    public void setNewOnlyOutBuffer(BufferedOutputStream newOnlyOutBuffer) {
	this.newOnlyOutBuffer = newOnlyOutBuffer;
    }

    public void setOldOnlyOutBuffer(BufferedOutputStream oldOnlyOutBuffer) {
	this.oldOnlyOutBuffer = oldOnlyOutBuffer;
    }

    public void setDiffColumnOutBuffer(BufferedOutputStream diffColumnOutBuffer) {
	this.diffColumnOutBuffer = diffColumnOutBuffer;
    }

    public MergeThread() {
    }

    public void run() {
	try {
	    merge();
	} catch (Exception e) {
	    loggerUtil.error(mergeTableInfo.getNewTableNameString(), e);
	    e.printStackTrace();
	}
    }

    private void merge() throws SQLException, IOException,
	    ClassNotFoundException {
	ResultSet newRs = null;
	ResultSet oldRs = null;

	MergeSQL mergeSQL = new MergeSQL(maxBucket, hashValue);
	String newSql = mergeSQL.getNewTableSql(mergeTableInfo);
	String oldSql = mergeSQL.getOldTableSql(mergeTableInfo);
	
	StringBuilder newKeyValues = new StringBuilder();
	StringBuilder oldKeyValues = new StringBuilder();
	String newColumnValue = "";
	String oldColumnValue = "";

	DBUtils dbUtils = new DBUtils(loggerUtil);
	dbUtils.initDBConn();

	newRs = dbUtils.excuteNewDb(newSql);
	oldRs = dbUtils.excuteOldDb(oldSql);

	boolean newExist = false;
	boolean oldExist = false;

	if (newRs != null) {
	    newExist = newRs.next();
	}
	if (oldRs != null) {
	    oldExist = oldRs.next();
	}

	while (newExist || oldExist) {

	    newKeyValues.setLength(0);
	    oldKeyValues.setLength(0);

	    if (newExist) {
		for (String newKeyColumn : mergeTableInfo
			.getNewTableKeyColumnList()) {
		    newKeyValues.append(",");
		    newKeyValues.append(newRs.getString(newKeyColumn));
		}
	    }

	    if (oldExist) {
		for (String oldKeyColumn : mergeTableInfo
			.getOldTableKeyColumnList()) {
		    oldKeyValues.append(",");
		    oldKeyValues.append(oldRs.getString(oldKeyColumn));
		}
	    }

	    // the small key will read next
	    Integer compareResultString = compare(new String(newKeyValues), new String(oldKeyValues));
	    if (compareResultString > 0) {
		outputOldOnly(oldRs, oldKeyValues);
		oldExist = oldRs.next();
	    } else if (compareResultString < 0) {
		outputNewOnly(newRs, newKeyValues);
		newExist = newRs.next();
	    } else {
		for (int i = 0; i < mergeTableInfo.getNewTableMergeColumnList()
			.size(); i++) {
		    newColumnValue = newRs.getString(mergeTableInfo
			    .getNewTableMergeColumnList().get(i));
		    oldColumnValue = oldRs.getString(mergeTableInfo
			    .getOldTableMergeColumnList().get(i));

		    if (notEqual(newColumnValue, oldColumnValue)) {
			outputDiffColumn(newRs, newKeyValues, mergeTableInfo
				.getNewTableMergeColumnList().get(i),
				newColumnValue, oldColumnValue);
		    }
		}

		newExist = newRs.next();
		oldExist = oldRs.next();
	    }
	}

	synchronized (lock) {
	    newOnlyOutBuffer.flush();
	    oldOnlyOutBuffer.flush();
	    diffColumnOutBuffer.flush();
	}

	// close db conn
	dbUtils.closeConn();
	if (newRs != null) {
	    newRs.close();
	    newRs = null;
	}
	if (oldRs != null) {
	    oldRs.close();
	    oldRs = null;
	}
    }

    private void outputNewOnly(ResultSet newRs, StringBuilder oldKeyValues)
	    throws SQLException, IOException {
	StringBuilder stringBuilder = new StringBuilder("mergeKey[");
	stringBuilder.append(mergeTableInfo.getOldTableKeyColumn());
	stringBuilder.append("]");
	stringBuilder.append("=[");
	stringBuilder.append(new String(oldKeyValues).replaceFirst(",",""));
	stringBuilder.append("]");

	synchronized (lock) {
	    newOnlyOutBuffer.write(new String(stringBuilder).getBytes(StandardCharsets.UTF_8));
	    newOnlyOutBuffer.write(System.lineSeparator().getBytes());
	    if (newRs.getRow() % 3000 == 0) {
		newOnlyOutBuffer.flush();
	    }
	}
    }

    private void outputOldOnly(ResultSet oldRs, StringBuilder newKeyValues)
	    throws SQLException, IOException {
	StringBuilder stringBuilder = new StringBuilder("mergeKey[");
	stringBuilder.append(mergeTableInfo.getNewTableKeyColumn());
	stringBuilder.append("]");
	stringBuilder.append("=[");
	stringBuilder.append(new String(newKeyValues).replaceFirst(",",""));
	stringBuilder.append("]");

	synchronized (lock) {
	    oldOnlyOutBuffer.write(new String(stringBuilder).getBytes(StandardCharsets.UTF_8));
	    oldOnlyOutBuffer.write(System.lineSeparator().getBytes());
	    if (oldRs.getRow() % 3000 == 0) {
		oldOnlyOutBuffer.flush();
	    }
	}
    }

    private void outputDiffColumn(ResultSet newRs, StringBuilder newKeyValues,
	    String newColumnName, String newColumnValue, String oldColumnValue)
	    throws SQLException, IOException {
	StringBuilder stringBuilder = new StringBuilder("mergeKey[");
	stringBuilder.append(mergeTableInfo.getNewTableKeyColumn());
	stringBuilder.append("]");
	stringBuilder.append("=[");
	stringBuilder.append(new String(newKeyValues).replaceFirst(",", ""));
	stringBuilder.append("],");

	stringBuilder.append(newColumnName);
	stringBuilder.append("=[");
	stringBuilder.append(newColumnValue);
	stringBuilder.append(",");
	stringBuilder.append(oldColumnValue);
	stringBuilder.append("]");

	synchronized (lock) {
	    diffColumnOutBuffer.write(new String(stringBuilder).getBytes(StandardCharsets.UTF_8));
	    diffColumnOutBuffer.write(System.lineSeparator().getBytes());
	    if (newRs.getRow() % 300 == 0) {
		diffColumnOutBuffer.flush();
	    }
	}
    }

    private boolean equal(String newStr, String oldStr) {
	boolean result = false;
	if (newStr == null) {
	    if (oldStr == null) {
		result = true;
	    } else {
		result = false;
	    }
	} else {
	    if (oldStr == null) {
		result = false;
	    } else {
		result = newStr.equals(oldStr);
	    }
	}
	return result;
    }

    private boolean notEqual(String newStr, String oldStr) {
	return !equal(newStr, oldStr);
    }

    private Integer compare(String newStr, String oldStr) {
	if (newStr == null || "".equals(newStr)) {
	    newStr = null;
	}
	if (oldStr == null || "".equals(oldStr)) {
	    oldStr = null;
	}
	
	Integer result = 0;
	if (newStr == null) {
	    if (oldStr == null) {
		result = 0;
	    } else {
		result = 1;
	    }
	} else {
	    if (oldStr == null) {
		result = -1;
	    } else {
		result = newStr.compareTo(oldStr);
	    }
	}
	return result;
    }

}

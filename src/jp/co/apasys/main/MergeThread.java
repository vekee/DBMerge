package jp.co.apasys.main;

import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import jp.co.apasys.model.MergeTableInfo;
import jp.co.apasys.table.MergeSQL;
import jp.co.apasys.utils.DBUtils;
import jp.co.apasys.utils.LoggerUtil;

public class MergeThread extends Thread {
	
	private Integer maxBucket = 10;
	private Integer hashValue = 1;
	private MergeTableInfo mergeTableInfo = null;
	private LoggerUtil loggerUtil = null;
	private BufferedWriter newOnlyBufferedWriter = null;
	private BufferedWriter oldOnlyBufferedWriter = null;
	private BufferedWriter diffColumnBufferedWriter = null;
	
	public Integer getMaxBucket() {
		return maxBucket;
	}
	public void setMaxBucket(Integer maxBucket) {
		this.maxBucket = maxBucket;
	}
	public Integer getHashValue() {
		return hashValue;
	}
	public void setHashValue(Integer hashValue) {
		this.hashValue = hashValue;
	}
	public MergeTableInfo getMergeTableInfo() {
		return mergeTableInfo;
	}
	public void setMergeTableInfo(MergeTableInfo mergeTableInfo) {
		this.mergeTableInfo = mergeTableInfo;
	}
	public LoggerUtil getLoggerUtil() {
		return loggerUtil;
	}
	public void setLoggerUtil(LoggerUtil loggerUtil) {
		this.loggerUtil = loggerUtil;
	}
	public BufferedWriter getNewOnlyBufferedWriter() {
		return newOnlyBufferedWriter;
	}
	public void setNewOnlyBufferedWriter(BufferedWriter newOnlyBufferedWriter) {
		this.newOnlyBufferedWriter = newOnlyBufferedWriter;
	}
	public BufferedWriter getOldOnlyBufferedWriter() {
		return oldOnlyBufferedWriter;
	}
	public void setOldOnlyBufferedWriter(BufferedWriter oldOnlyBufferedWriter) {
		this.oldOnlyBufferedWriter = oldOnlyBufferedWriter;
	}
	public BufferedWriter getDiffColumnBufferedWriter() {
		return diffColumnBufferedWriter;
	}
	public void setDiffColumnBufferedWriter(BufferedWriter diffColumnBufferedWriter) {
		this.diffColumnBufferedWriter = diffColumnBufferedWriter;
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

	private void merge() throws SQLException, IOException {
		ResultSet newRs = null;
		ResultSet oldRs = null;
		
		MergeSQL mergeSQL = new MergeSQL(maxBucket,hashValue);
		String newSql = mergeSQL.getNewTableSql(mergeTableInfo);
		String oldSql = mergeSQL.getOldTableSql(mergeTableInfo);

		
		StringBuilder newKeyValues = new StringBuilder();
		StringBuilder oldKeyValues = new StringBuilder();
		String newColumnValue = "";
		String oldColumnValue = "";

                DBUtils dbUtils = new DBUtils();
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


			if (newRs != null) {
				for (String newKeyColumn : mergeTableInfo.getNewTableKeyColumnList()) {
					newKeyValues.append(",");
					newKeyValues.append(newRs.getString(newKeyColumn));
				}
			}
			
			if (oldRs != null) {
				for (String oldKeyColumn : mergeTableInfo.getOldTableKeyColumnList()) {
					oldKeyValues.append(",");
					oldKeyValues.append(oldRs.getString(oldKeyColumn));
				}
			}
			
			if (new String(newKeyValues).compareTo(new String(oldKeyValues)) > 0) {
				synchronized (lock) {outputOldOnly(oldRs);}
				
				oldExist = oldRs.next();
			} else if (new String(newKeyValues).compareTo(new String(oldKeyValues)) < 0) {
				synchronized (lock) {outputNewOnly(newRs);}

				newExist = newRs.next();
			} else {
				for (int i=0;i<mergeTableInfo.getNewTableMergeColumnList().size();i++) {
					newColumnValue = newRs.getString(mergeTableInfo.getNewTableMergeColumnList().get(i));
					oldColumnValue = newRs.getString(mergeTableInfo.getOldTableMergeColumnList().get(i));
					
					if (notEqual(newColumnValue, oldColumnValue)) {
						synchronized(lock){outputDiffColumn(newRs,mergeTableInfo.getNewTableMergeColumnList().get(i),newColumnValue,mergeTableInfo.getOldTableMergeColumnList().get(i),oldColumnValue);}
					}
					
					
					newExist = newRs.next();
					
					oldExist = oldRs.next();
				}
			}
		}
		oldRs = dbUtils.excuteNewDb(oldSql);
		
	}
	
	private void outputNewOnly(ResultSet newRs) throws SQLException, IOException {
		StringBuilder stringBuilder = new StringBuilder("mergeKey[");
		for (String newKeyColumn : mergeTableInfo.getNewTableKeyColumnList()) {
			stringBuilder.append(",");
			stringBuilder.append(newKeyColumn);
			stringBuilder.append("=");;
			stringBuilder.append(newRs.getString(newKeyColumn));
		}
		stringBuilder.append("]");
		
		newOnlyBufferedWriter.write(new String(stringBuilder).replaceFirst(",", ""));
		newOnlyBufferedWriter.newLine();
	}
	
	private void outputOldOnly(ResultSet oldRs) throws SQLException, IOException {
		StringBuilder stringBuilder = new StringBuilder("mergeKey[");
		for (String oldKeyColumn : mergeTableInfo.getOldTableKeyColumnList()) {
			stringBuilder.append(",");
			stringBuilder.append(oldKeyColumn);
			stringBuilder.append("=");;
			stringBuilder.append(oldRs.getString(oldKeyColumn));
		}
		stringBuilder.append("]");
		
		oldOnlyBufferedWriter.write(new String(stringBuilder).replaceFirst(",", ""));
		oldOnlyBufferedWriter.newLine();
	}
	
	private void outputDiffColumn(ResultSet newRs, String newColumn, String newColumnValue, String oldColumn, String oldColumnValue) throws SQLException, IOException {
		StringBuilder stringBuilder = new StringBuilder("mergeKey[");
		for (String newKeyColumn : mergeTableInfo.getNewTableKeyColumnList()) {
			stringBuilder.append(",");
			stringBuilder.append(newKeyColumn);
			stringBuilder.append("=");;
			stringBuilder.append(newRs.getString(newKeyColumn));
		}
		stringBuilder.append("],Values[");
		stringBuilder.append(newColumn);
		stringBuilder.append("=");
		stringBuilder.append(newColumnValue);
		stringBuilder.append(",");
		stringBuilder.append(oldColumn);
		stringBuilder.append("=");
		stringBuilder.append(oldColumnValue);
		stringBuilder.append("]");
		
		diffColumnBufferedWriter.write(new String(stringBuilder).replaceFirst(",", ""));
		diffColumnBufferedWriter.newLine();
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

}

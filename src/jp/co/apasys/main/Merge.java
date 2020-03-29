package jp.co.apasys.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.CopyOnWriteArraySet;

import jp.co.apasys.model.MergeTableInfo;
import jp.co.apasys.table.MergeTable;
import jp.co.apasys.table.MergeTableCollector;
import jp.co.apasys.utils.DBUtils;
import jp.co.apasys.utils.LoggerUtil;

public class Merge implements Runnable {
	
	private static String outputDir = "./output";
	
	private Integer mainThreadCount = 10;
	private Integer mergeThreadCount = 10;
	private LoggerUtil loggerUtil = null;
	private MergeTableCollector mergeTableCollector = null;

	public Merge() {
	}
	
	public Integer getMainThreadCount() {
		return mainThreadCount;
	}
	public void setMainThreadCount(Integer mainThreadCount) {
		this.mainThreadCount = mainThreadCount;
	}
	public Integer getMergeThreadCount() {
		return mergeThreadCount;
	}
	public void setMergeThreadCount(Integer mergeThreadCount) {
		this.mergeThreadCount = mergeThreadCount;
	}
	public LoggerUtil getLoggerUtil() {
		return loggerUtil;
	}
	public void setLoggerUtil(LoggerUtil loggerUtil) {
		this.loggerUtil = loggerUtil;
	}
	public MergeTableCollector getMergeTableCollector() {
		return mergeTableCollector;
	}
	public void setMergeTableCollector(MergeTableCollector mergeTableCollector) {
		this.mergeTableCollector = mergeTableCollector;
	}
	

	public void run() {
		CopyOnWriteArraySet<MergeThread> threadSet = new CopyOnWriteArraySet<MergeThread>();
		String mergeTableFile = "";
		MergeThread mergeThread = null;
		DBUtils dbUtils = null;
		MergeTableInfo mergeTableInfo = null;
		
		while (MergeTableCollector.hasNext()) {
			
			try {
				mergeTableFile = MergeTableCollector.next();
			    mergeTableInfo = MergeTable.getMergeTableInfo(mergeTableFile);
			    
				BufferedWriter newOnlyBufferedWriter = new BufferedWriter(new FileWriter(new File(outputDir + File.separator + mergeTableInfo.getNewTableNameString() + "_newOnly.txt")));
				BufferedWriter oldOnlyBufferedWriter = new BufferedWriter(new FileWriter(new File(outputDir + File.separator + mergeTableInfo.getNewTableNameString() + "_oldOnly.txt")));
				BufferedWriter diffColumnBufferedWriter = new BufferedWriter(new FileWriter(new File(outputDir + File.separator + mergeTableInfo.getNewTableNameString() + "_diffColumn.txt")));
				
				for(int i=1;i<=mergeThreadCount;i++) {
					dbUtils = new DBUtils();
					mergeThread = new MergeThread();
					mergeThread.setLoggerUtil(loggerUtil);
					mergeThread.setDbUtils(dbUtils);
					mergeThread.setMergeTableInfo(mergeTableInfo);
					mergeThread.setMaxBucket(mainThreadCount);
					mergeThread.setHashValue(i);
					mergeThread.setNewOnlyBufferedWriter(newOnlyBufferedWriter);
					mergeThread.setOldOnlyBufferedWriter(oldOnlyBufferedWriter);
					mergeThread.setDiffColumnBufferedWriter(diffColumnBufferedWriter);
					
					mergeThread.start();
					
					threadSet.add(mergeThread);
				}
			} catch (Exception e) {
				loggerUtil.error(mergeTableFile, e);
			}
			
			// read next table file after all subThread is finished
			waitForMergeThreadFinished(threadSet);
			
		}
		
		// progrem is over after all subThread is finished
		waitForMergeThreadFinished(threadSet);
		
	}
	
	private void waitForMergeThreadFinished(CopyOnWriteArraySet<MergeThread> threadSet) {
		while (true) {
			boolean finishFlag = true;
			for (MergeThread mergeThreadItem : threadSet) {
				if (mergeThreadItem.isAlive()) {
					finishFlag = false;
					break;
				} else {
					threadSet.remove(mergeThreadItem);
				}
			}
			
			if (finishFlag) {
				break;
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

}

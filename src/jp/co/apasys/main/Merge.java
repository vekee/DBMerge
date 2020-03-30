package jp.co.apasys.main;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.concurrent.CopyOnWriteArraySet;

import jp.co.apasys.model.MergeTableInfo;
import jp.co.apasys.table.MergeTable;
import jp.co.apasys.table.MergeTableCollector;
import jp.co.apasys.utils.LoggerUtil;

public class Merge implements Runnable {

    private static String outputDir = "./output/";

    private Integer mergeThreadCount = 10;
    private LoggerUtil loggerUtil = null;
    private MergeTableCollector mergeTableCollector = null;

    public Merge() {
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
	MergeTableInfo mergeTableInfo = null;
	BufferedOutputStream newOnlyOutBuffer = null;
	BufferedOutputStream oldOnlyOutBuffer = null;
	BufferedOutputStream diffColumnOutBuffer = null;

	while (MergeTableCollector.hasNext()) {

	    try {
		mergeTableFile = MergeTableCollector.next();
		mergeTableInfo = MergeTable.getMergeTableInfo(mergeTableFile);

		newOnlyOutBuffer = new BufferedOutputStream(
			new FileOutputStream(outputDir
				+ mergeTableInfo.getNewTableNameString()
				+ "_newOnly.txt"));
		oldOnlyOutBuffer = new BufferedOutputStream(
			new FileOutputStream(outputDir
				+ mergeTableInfo.getNewTableNameString()
				+ "_oldOnly.txt"));
		diffColumnOutBuffer = new BufferedOutputStream(
			new FileOutputStream(outputDir
				+ mergeTableInfo.getNewTableNameString()
				+ "_diffColumn.txt"));

		for (int i = 1; i <= mergeThreadCount; i++) {

		    mergeThread = new MergeThread();
		    mergeThread.setLoggerUtil(loggerUtil);

		    mergeThread.setMergeTableInfo(mergeTableInfo);
		    mergeThread.setMaxBucket(mergeThreadCount);
		    mergeThread.setHashValue(i);
		    mergeThread.setNewOnlyOutBuffer(newOnlyOutBuffer);
		    mergeThread.setOldOnlyOutBuffer(oldOnlyOutBuffer);
		    mergeThread.setDiffColumnOutBuffer(diffColumnOutBuffer);

		    mergeThread.start();

		    threadSet.add(mergeThread);
		}

		// read next table file after all subThread is finished
		waitForMergeThreadFinished(threadSet);

	    } catch (Exception e) {
		loggerUtil.error(mergeTableFile, e);
	    } finally {
		try {
        		if (newOnlyOutBuffer != null) {
        		    newOnlyOutBuffer.flush();
        		    newOnlyOutBuffer.close();
        		}
        		if (newOnlyOutBuffer != null) {
        		    newOnlyOutBuffer.flush();
        		    newOnlyOutBuffer.close();
        		}
        		if (newOnlyOutBuffer != null) {
        		    newOnlyOutBuffer.flush();
        		    newOnlyOutBuffer.close();
        		}
		} catch (Exception e) {
		    loggerUtil.error("failed at closing merge result file", e);
		}
	    }
	}
    }

    private void waitForMergeThreadFinished(
	    CopyOnWriteArraySet<MergeThread> threadSet) throws InterruptedException {
	while (!threadSet.isEmpty()) {
	    for (MergeThread mergeThreadItem : threadSet) {
		if (!mergeThreadItem.isAlive()) {
		    threadSet.remove(mergeThreadItem);
		}
	    }
	    Thread.sleep(1000);
	}
    }

}

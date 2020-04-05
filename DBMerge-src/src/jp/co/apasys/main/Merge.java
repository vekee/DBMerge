package jp.co.apasys.main;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.concurrent.CopyOnWriteArraySet;

import jp.co.apasys.table.MergeTableCollector;
import jp.co.apasys.utils.LoggerUtil;

public class Merge implements Runnable {

    private static String outputDir = "./output/";

    private Integer mergeThreadCount = 10;
    private LoggerUtil loggerUtil = null;
    private MergeTableCollector mergeTableCollector = null;

    public Merge() {
    }

    public void setMergeThreadCount(Integer mergeThreadCount) {
	this.mergeThreadCount = mergeThreadCount;
    }

    public void setLoggerUtil(LoggerUtil loggerUtil) {
	this.loggerUtil = loggerUtil;
    }

    public void setMergeTableCollector(MergeTableCollector mergeTableCollector) {
	this.mergeTableCollector = mergeTableCollector;
    }

    public void run() {
	CopyOnWriteArraySet<MergeThread> threadSet = new CopyOnWriteArraySet<MergeThread>();
	String mergeTableFile = "";
	MergeThread mergeThread = null;
	BufferedOutputStream newOnlyOutBuffer = null;
	BufferedOutputStream oldOnlyOutBuffer = null;
	BufferedOutputStream diffColumnOutBuffer = null;

	while (true) {

	    mergeTableFile = this.mergeTableCollector.next();
	    if (mergeTableFile == null || mergeTableFile.equals("")) {
		break;
	    }

	    try {

		newOnlyOutBuffer = new BufferedOutputStream(
			new FileOutputStream(outputDir
				+ mergeTableFile.split("¥.")[0]
				+ "_newOnly.txt"));
		oldOnlyOutBuffer = new BufferedOutputStream(
			new FileOutputStream(outputDir
				+ mergeTableFile.split("¥.")[0]
				+ "_oldOnly.txt"));
		diffColumnOutBuffer = new BufferedOutputStream(
			new FileOutputStream(outputDir
				+ mergeTableFile.split("¥.")[0]
				+ "_diffColumn.txt"));

		for (int i = 1; i <= mergeThreadCount; i++) {

		    mergeThread = new MergeThread();
		    mergeThread.setLoggerUtil(loggerUtil);

		    mergeThread.setMergeTableInfo(mergeTableFile);
		    mergeThread.setMaxBucket(mergeThreadCount);
		    mergeThread.setHashValue(i);
		    mergeThread.setNewOnlyOutBuffer(newOnlyOutBuffer);
		    mergeThread.setOldOnlyOutBuffer(oldOnlyOutBuffer);
		    mergeThread.setDiffColumnOutBuffer(diffColumnOutBuffer);

		    mergeThread.setName("mergeThread" + i);
		    mergeThread.start();

		    threadSet.add(mergeThread);
		}

		// read next table file after all subThread is finished
		waitForMergeThreadFinished(threadSet);
		
	    } catch (Exception e) {
		loggerUtil.error(mergeTableFile, e);
	    }
	}
    }

    private void waitForMergeThreadFinished(
	    CopyOnWriteArraySet<MergeThread> threadSet)
	    throws InterruptedException {
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

package jp.co.apasys.main;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;

import jp.co.apasys.table.MergeTableCollector;
import jp.co.apasys.utils.FileUtils;
import jp.co.apasys.utils.LoggerUtil;

public class Execute {

    private static String outputDir = "./output/";
    private static String logFile = "./output/Merge.log";

    public static void main(String[] args) throws IOException, InterruptedException {

	FileUtils.backupResult(outputDir);

	Integer mainThreadCount = 10;
	Integer mergeThreadCount = 10;

	if (args != null && args.length > 0) {
	    mainThreadCount = Integer.parseInt(args[0]);
	}
	if (args != null && args.length > 1) {
	    mainThreadCount = Integer.parseInt(args[1]);
	}

	CopyOnWriteArraySet<Thread> mergeSet = new CopyOnWriteArraySet<Thread>();
	Merge merge = null;
	LoggerUtil loggerUtil = new LoggerUtil(logFile);
	// init the mergeTableCollector
	MergeTableCollector mergeTableCollector = new MergeTableCollector();

	loggerUtil.info("START DBMerge!");

	for (int i = 1; i <= mainThreadCount; i++) {
	    merge = new Merge();
	    merge.setLoggerUtil(loggerUtil);
	    merge.setMergeTableCollector(mergeTableCollector);
	    merge.setMergeThreadCount(mergeThreadCount);
	    Thread mainThread = new Thread(merge);
	    mainThread.setName("mainThread" + i);
	    mainThread.start();

	    mergeSet.add(mainThread);
	}

	waitForMainThreadFinished(mergeSet);
	
	loggerUtil.info("FINISH DBMerge!");

    }

    private static void waitForMainThreadFinished(
	    CopyOnWriteArraySet<Thread> mergeSet)
	    throws InterruptedException {
	while (!mergeSet.isEmpty()) {
	    for (Thread mainThread : mergeSet) {
		if (!mainThread.isAlive()) {
		    mergeSet.remove(mainThread);
		}
	    }
	    Thread.sleep(1000);
	}
    }
}

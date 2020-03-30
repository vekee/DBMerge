package jp.co.apasys.main;

import java.io.IOException;

import jp.co.apasys.table.MergeTableCollector;
import jp.co.apasys.utils.FileUtils;
import jp.co.apasys.utils.LoggerUtil;

public class Excute {

    private static String outputDir = "./output/";
    private static String logFile = "./output/Merge.log";

    public static void main(String[] args) throws IOException {

	FileUtils.backupResult(outputDir);

	Integer mainThreadCount = 10;
	Integer mergeThreadCount = 10;

	if (args != null && args.length > 0) {
	    mainThreadCount = Integer.parseInt(args[0]);
	}
	if (args != null && args.length > 1) {
	    mainThreadCount = Integer.parseInt(args[1]);
	}

	Merge merge = null;
	LoggerUtil loggerUtil = new LoggerUtil(logFile);
	MergeTableCollector mergeTableCollector = new MergeTableCollector();

	for (int i = 0; i < mainThreadCount; i++) {
	    merge = new Merge();
	    merge.setLoggerUtil(loggerUtil);
	    merge.setMergeTableCollector(mergeTableCollector);
	    merge.setMergeThreadCount(mergeThreadCount);
	    new Thread(merge).start();
	}
    }
}

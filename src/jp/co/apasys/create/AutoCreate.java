package jp.co.apasys.create;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.concurrent.CopyOnWriteArraySet;

import jp.co.apasys.utils.DBUtils;
import jp.co.apasys.utils.FileUtils;
import jp.co.apasys.utils.LoggerUtil;

public class AutoCreate {

    private static String outputDir = "./tables/";
    private static String logFile = "./output/AutoCreate.log";

    public static void main(String[] args) throws IOException {

	// backup table properties into old folder
	FileUtils.backupResult(outputDir);
	ResultSet newRs = null;
	AutoCreateThread autoCreateThread = null;
	CopyOnWriteArraySet<AutoCreateThread> threadSet = new CopyOnWriteArraySet<AutoCreateThread>();
	LoggerUtil loggerUtil = new LoggerUtil(logFile);

	try {

	    loggerUtil.info("start AutoCreate!");
	    DBUtils dbUtils = new DBUtils();
	    dbUtils.initDBConn();
	    newRs = dbUtils.getNewDBTables();

	    while (newRs.next()) {
		autoCreateThread = new AutoCreateThread(
			newRs.getString("TABLE_NAME"));
		autoCreateThread.setLoggerUtil(loggerUtil);
		new Thread(autoCreateThread).start();

		threadSet.add(autoCreateThread);

		// min and max create thread count is 50~100
		if (newRs.getRow() % 100 == 0) {
		    waitForCreateTablesThread(threadSet);
		}
	    }

	    // to finish after create all table properties file
	    waitForCreateTablesThreadFinished(threadSet);
	    loggerUtil.info("finished AutoCreate!");

	} catch (Exception e) {
	    loggerUtil.error("エラーが発生しました！", e);
	} finally {

	}
    }

    private static void waitForCreateTablesThreadFinished(
	    CopyOnWriteArraySet<AutoCreateThread> threadSet)
	    throws InterruptedException {
	while (!threadSet.isEmpty()) {
	    for (AutoCreateThread CreateTablesThreadItem : threadSet) {
		if (!CreateTablesThreadItem.isAlive()) {
		    threadSet.remove(CreateTablesThreadItem);
		}
	    }
	    Thread.sleep(100);
	}
    }

    private static void waitForCreateTablesThread(
	    CopyOnWriteArraySet<AutoCreateThread> threadSet)
	    throws InterruptedException {
	while (threadSet.size() > 50) {
	    for (AutoCreateThread CreateTablesThreadItem : threadSet) {
		if (!CreateTablesThreadItem.isAlive()) {
		    threadSet.remove(CreateTablesThreadItem);
		}
	    }
	    Thread.sleep(30);
	}
    }
}

package jp.co.apasys.create;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.concurrent.CopyOnWriteArraySet;

import jp.co.apasys.utils.DBUtils;
import jp.co.apasys.utils.FileUtils;
import jp.co.apasys.utils.LoggerUtil;

public class AutoCreate {

    private static String outputDir = "./tables/";
    private static String logFile = "./tables/AutoCreate.log";

    public static void main(String[] args) throws IOException {

	// backup table properties into old folder
	FileUtils.backupResult(outputDir);
	ResultSet newRs = null;
	AutoCreateThread autoCreateThread = null;
	CopyOnWriteArraySet<AutoCreateThread> threadSet = new CopyOnWriteArraySet<AutoCreateThread>();
	LoggerUtil loggerUtil = new LoggerUtil(logFile);

	try {

	    loggerUtil.info("start AutoCreate!");
	    DBUtils dbUtils = new DBUtils(loggerUtil);
	    dbUtils.initDBConn();
	    newRs = dbUtils.getNewDBTables();

	    while (newRs.next()) {
		autoCreateThread = new AutoCreateThread(
			newRs.getString("TABLE_NAME"));
		autoCreateThread.setLoggerUtil(loggerUtil);
		new Thread(autoCreateThread).start();

		threadSet.add(autoCreateThread);

		// min and max create thread count is 10~50
		if (threadSet.size() > 50) {
		    waitForCreateTablesThread(threadSet);
		}
	    }

	    // close connect and resultSet
	    dbUtils.closeConn();
	    newRs.close();

	    // to finish after create all table properties file
	    waitForCreateTablesThreadFinished(threadSet);

	} catch (Exception e) {
	    loggerUtil.error("exception", e.getCause());
	}

	loggerUtil.info("finished AutoCreate!");

    }

    private static void waitForCreateTablesThreadFinished(
	    CopyOnWriteArraySet<AutoCreateThread> threadSet)
	    throws InterruptedException {
	while (!threadSet.isEmpty()) {
	    for (AutoCreateThread autoCreateThread : threadSet) {
		if (!autoCreateThread.isAlive()) {
		    threadSet.remove(autoCreateThread);
		}
	    }
	    Thread.sleep(500);
	}
    }

    private static void waitForCreateTablesThread(
	    CopyOnWriteArraySet<AutoCreateThread> threadSet)
	    throws InterruptedException {
	while (threadSet.size() > 10) {
	    for (AutoCreateThread autoCreateThread : threadSet) {
		if (!autoCreateThread.isAlive()) {
		    threadSet.remove(autoCreateThread);
		}
	    }
	    Thread.sleep(1000);
	}
    }
}

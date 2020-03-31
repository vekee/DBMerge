package jp.co.apasys.utils;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LoggerUtil {
	private final static Object lock = new Object();
	private static String logFile = "./output/DBMerge.log";

	Logger logger = null;

	public LoggerUtil() {
		try {
			this.logger = Logger.getLogger("");
			intiLoger();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public LoggerUtil(String logFile) {
		LoggerUtil.logFile = logFile;
		this.logger = Logger.getLogger("");
		try {
			intiLoger();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public LoggerUtil(String logFile, String className) {
		LoggerUtil.logFile = logFile;
		this.logger = Logger.getLogger(className);
		try {
			intiLoger();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void intiLoger() throws SecurityException, IOException {
		FileHandler fHandler = new FileHandler(LoggerUtil.logFile, true);
		fHandler.setFormatter(new SimpleFormatter());
		logger.addHandler(fHandler);
	}

	public void info(String msg) {
		synchronized (lock) {
			logger.info(msg);
		}
	}

	public void error(String msg, Throwable thrown) {
		synchronized (lock) {
			logger.log(Level.FINEST, msg, thrown);
		}
	}

}

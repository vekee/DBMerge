package jp.co.apasys.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBUtils {
	private static String jdbcConfigFile = "./config/jdbc.properties";
	
	private static String newDbDriver = "new.db.driver";
	private static String newDbUrl = "new.db.url";
	private static String newDbUsername = "new.db.username";
	private static String newDbPassword = "new.db.password";
	private static String oldDbDriver = "old.db.driver";
	private static String oldDbUrl = "old.db.url";
	private static String oldDbUsername = "old.db.username";
	private static String oldDbPassword = "old.db.password";
	
	private Properties properties = new Properties();
	private Connection newConn = null;
	private Statement newStmt = null;
	private ResultSet newRs = null;
	
	private Connection oldConn = null;
	private Statement oldStmt = null;
	private ResultSet oldRs = null;
	
	public DBUtils() throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		properties.load(new FileInputStream(jdbcConfigFile));
		
		Class.forName(properties.getProperty(newDbDriver));
		newConn = DriverManager.getConnection(properties.getProperty(newDbUrl),
				properties.getProperty(newDbUsername),
				properties.getProperty(newDbPassword));
		Class.forName(properties.getProperty(oldDbDriver));
		oldConn = DriverManager.getConnection(properties.getProperty(oldDbUrl),
				properties.getProperty(oldDbUsername),
				properties.getProperty(oldDbPassword));
		
	}
	
	public ResultSet excuteNewDb(String sql) {
		try {
			newStmt = newConn.createStatement();
			newStmt.setFetchSize(1000);
			newRs = newStmt.executeQuery(sql);
		} catch (SQLException e) {
			newRs = null;
			e.printStackTrace();
		}
		
		return newRs;
	}
	
	public ResultSet excuteOldDb(String sql) {
		try {
			oldStmt = oldConn.createStatement();
			oldStmt.setFetchSize(1000);
			oldRs = oldStmt.executeQuery(sql);
		} catch (SQLException e) {
			oldRs = null;
			e.printStackTrace();
		}
		
		return oldRs;
	}
	
	public void closeConn() throws SQLException {
	    if (newRs != null) {
	    	newRs.close();
	    }
	    if (newStmt != null) {
	    	newStmt.close();
	    }
	    if (newConn != null) {
	    	newConn.close();
	    }
	    
	    if (oldRs != null) {
	    	oldRs.close();
	    }
	    if (oldStmt != null) {
	    	oldStmt.close();
	    }
	    if (oldConn != null) {
	    	oldConn.close();
	    }
	}
}

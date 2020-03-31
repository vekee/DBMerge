package jp.co.apasys.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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

	private static String newCreateSchemaName = "new.create.schema.name";
	private static String oldCreateSchemaName = "old.create.schema.name";
	private static String commCreateTableName = "comm.create.table.name";

	private Properties properties = new Properties();
	private Connection newConn = null;
	private Statement newStmt = null;
	private ResultSet newRs = null;
	private DatabaseMetaData newMetadata = null;

	private Connection oldConn = null;
	private Statement oldStmt = null;
	private ResultSet oldRs = null;
	private DatabaseMetaData oldMetadata = null;

	public void initDBConn() throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		properties.load(new FileInputStream(jdbcConfigFile));

		Class.forName(properties.getProperty(newDbDriver));
		newConn = DriverManager.getConnection(properties.getProperty(newDbUrl), properties.getProperty(newDbUsername),
				properties.getProperty(newDbPassword));

		this.newStmt = this.newConn.createStatement();
		this.newStmt.setFetchSize(1000);

		this.newMetadata = this.newConn.getMetaData();

		Class.forName(properties.getProperty(oldDbDriver));
		oldConn = DriverManager.getConnection(properties.getProperty(oldDbUrl), properties.getProperty(oldDbUsername),
				properties.getProperty(oldDbPassword));

		this.oldStmt = this.oldConn.createStatement();
		this.oldStmt.setFetchSize(1000);

		this.oldMetadata = this.oldConn.getMetaData();

	}

	public ResultSet excuteNewDb(String sql) {
		try {
			newRs = newStmt.executeQuery(sql);
		} catch (SQLException e) {
			newRs = null;
			e.printStackTrace();
		}

		return newRs;
	}

	public ResultSet excuteOldDb(String sql) {
		try {
			oldRs = oldStmt.executeQuery(sql);
		} catch (SQLException e) {
			oldRs = null;
			e.printStackTrace();
		}

		return oldRs;
	}

	public ResultSet getNewDBTables() throws SQLException {
		return this.newMetadata.getTables(null, properties.getProperty(newCreateSchemaName),
				properties.getProperty(commCreateTableName), new String[] { "TABLE_NAME" });
	}

	public ResultSet getOldDBTables() throws SQLException {
		return this.oldMetadata.getTables(null, properties.getProperty(oldCreateSchemaName),
				properties.getProperty(commCreateTableName), new String[] { "TABLE_NAME" });
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

	public DatabaseMetaData getNewMetadata() {
		return newMetadata;
	}

	public void setNewMetadata(DatabaseMetaData newMetadata) {
		this.newMetadata = newMetadata;
	}

	public DatabaseMetaData getOldMetadata() {
		return oldMetadata;
	}

	public void setOldMetadata(DatabaseMetaData oldMetadata) {
		this.oldMetadata = oldMetadata;
	}

	public String getNewCreateSchemaName() {
		return properties.getProperty(newCreateSchemaName);
	}

	public String getOldCreateSchemaName() {
		return properties.getProperty(oldCreateSchemaName);
	}

	public String getCommCreateTableName() {
		return properties.getProperty(commCreateTableName);
	}

}

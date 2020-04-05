package jp.co.apasys.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import jp.co.apasys.model.JDBC;

public class DBUtils {

    private Connection newConn = null;
    private Statement newStmt = null;
    private ResultSet newRs = null;
    private DatabaseMetaData newMetadata = null;

    private Connection oldConn = null;
    private Statement oldStmt = null;
    private ResultSet oldRs = null;
    private DatabaseMetaData oldMetadata = null;

    private LoggerUtil loggerUtil = null;
    
    public JDBC jdbc = null;

    public DBUtils(LoggerUtil loggerUtil) throws FileNotFoundException,
	    IOException {
	this.loggerUtil = loggerUtil;
	this.jdbc = LoadJDBC.load();
    }

    public void initDBConn() throws FileNotFoundException, IOException,
	    ClassNotFoundException, SQLException {

	Class.forName(jdbc.getNewDbDriver());
	newConn = DriverManager.getConnection(jdbc.getNewDbUrl(),
		jdbc.getNewDbUsername(), jdbc.getNewDbPassword());
	newMetadata = newConn.getMetaData();

	this.newStmt = this.newConn.createStatement();
	this.newStmt.setFetchSize(1000);

	Class.forName(jdbc.getOldDbDriver());
	oldConn = DriverManager.getConnection(jdbc.getOldDbUrl(),
		jdbc.getOldDbUsername(), jdbc.getOldDbPassword());
	oldMetadata = oldConn.getMetaData();

	this.oldStmt = this.oldConn.createStatement();
	this.oldStmt.setFetchSize(1000);
    }

    public ResultSet excuteNewDb(String sql) {
	try {
	    newRs = newStmt.executeQuery(sql);
	} catch (SQLException e) {
	    newRs = null;
	    loggerUtil.error(sql, e.getCause());
	    e.printStackTrace();
	}

	return newRs;
    }

    public ResultSet excuteOldDb(String sql) {
	try {
	    oldRs = oldStmt.executeQuery(sql);
	} catch (SQLException e) {
	    oldRs = null;
	    loggerUtil.error(sql, e.getCause());
	    e.printStackTrace();
	}

	return oldRs;
    }

    public ResultSet getNewDBTables() throws SQLException {
	return newMetadata.getTables(null, jdbc.getNewCreateSchemaName(),
	jdbc.getCommCreateTableName(), null);
    }

    public ResultSet getOldDBTables() throws SQLException {
	return oldMetadata.getTables(null, jdbc.getOldCreateSchemaName(),
		jdbc.getCommCreateTableName(), null);
    }
    
    public ResultSet getNewColumns(String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern) throws SQLException {
	return newMetadata.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }
    
    public ResultSet getOldColumns(String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern) throws SQLException {
	return oldMetadata.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }
    
    public ResultSet getNewPrimaryKeys(String catalog, String schema, String table) throws SQLException {
	return newMetadata.getPrimaryKeys(catalog, schema, table);
    }
    
    public ResultSet getOldPrimaryKeys(String catalog, String schema, String table) throws SQLException {
	return oldMetadata.getPrimaryKeys(catalog, schema, table);
    }

    public void closeConn() throws SQLException {
	if (newRs != null) {
	    newRs.close();
	    newRs = null;
	}
	if (newStmt != null) {
	    newStmt.close();
	    newStmt = null;
	}
	if (newConn != null) {
	    newConn.close();
	    newConn = null;
	}

	if (oldRs != null) {
	    oldRs.close();
	    oldRs = null;
	}
	if (oldStmt != null) {
	    oldStmt.close();
	    oldStmt = null;
	}
	if (oldConn != null) {
	    oldConn.close();
	    oldConn = null;
	}
    }

}

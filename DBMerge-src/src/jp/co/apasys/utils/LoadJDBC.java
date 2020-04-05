package jp.co.apasys.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import jp.co.apasys.model.JDBC;

public class LoadJDBC {
    private static final String jdbcConfigFile = "./config/jdbc.properties";
    
    private static final String newDbDriver = "new.db.driver";
    private static final String newDbUrl = "new.db.url";
    private static final String newDbUsername = "new.db.username";
    private static final String newDbPassword = "new.db.password";

    private static final String oldDbDriver = "old.db.driver";
    private static final String oldDbUrl = "old.db.url";
    private static final String oldDbUsername = "old.db.username";
    private static final String oldDbPassword = "old.db.password";

    private static final String newCreateSchemaName = "new.create.schema.name";
    private static final String oldCreateSchemaName = "old.create.schema.name";
    private static final String commCreateTableName = "comm.create.table.name";

    public static JDBC load() throws FileNotFoundException, IOException {
	JDBC jdbc = new JDBC();
	Properties properties = new Properties();
	properties.load(new FileInputStream(jdbcConfigFile));

	jdbc.setNewDbDriver(properties.getProperty(newDbDriver));
	jdbc.setNewDbUrl(properties.getProperty(newDbUrl));
	jdbc.setNewDbUsername(properties.getProperty(newDbUsername));
	jdbc.setNewDbPassword(properties.getProperty(newDbPassword));
	
	jdbc.setOldDbDriver(properties.getProperty(oldDbDriver));
	jdbc.setOldDbUrl(properties.getProperty(oldDbUrl));
	jdbc.setOldDbUsername(properties.getProperty(oldDbUsername));
	jdbc.setOldDbPassword(properties.getProperty(oldDbPassword));
	
	jdbc.setNewCreateSchemaName(properties.getProperty(newCreateSchemaName));
	jdbc.setOldCreateSchemaName(properties.getProperty(oldCreateSchemaName));
	jdbc.setCommCreateTableName(properties.getProperty(commCreateTableName));
	
	return jdbc;
    }
}

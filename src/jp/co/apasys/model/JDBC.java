package jp.co.apasys.model;

public class JDBC {
    
    private String newDbDriver = "";
    private String newDbUrl = "";
    private String newDbUsername = "";
    private String newDbPassword = "";

    private String oldDbDriver = "";
    private String oldDbUrl = "";
    private String oldDbUsername = "";
    private String oldDbPassword = "";

    private String newCreateSchemaName = "";
    private String oldCreateSchemaName = "";
    private String commCreateTableName = "";
    
    public String getNewDbDriver() {
        return newDbDriver;
    }
    public void setNewDbDriver(String newDbDriver) {
        this.newDbDriver = newDbDriver;
    }
    public String getNewDbUrl() {
        return newDbUrl;
    }
    public void setNewDbUrl(String newDbUrl) {
        this.newDbUrl = newDbUrl;
    }
    public String getNewDbUsername() {
        return newDbUsername;
    }
    public void setNewDbUsername(String newDbUsername) {
        this.newDbUsername = newDbUsername;
    }
    public String getNewDbPassword() {
        return newDbPassword;
    }
    public void setNewDbPassword(String newDbPassword) {
        this.newDbPassword = newDbPassword;
    }
    public String getOldDbDriver() {
        return oldDbDriver;
    }
    public void setOldDbDriver(String oldDbDriver) {
        this.oldDbDriver = oldDbDriver;
    }
    public String getOldDbUrl() {
        return oldDbUrl;
    }
    public void setOldDbUrl(String oldDbUrl) {
        this.oldDbUrl = oldDbUrl;
    }
    public String getOldDbUsername() {
        return oldDbUsername;
    }
    public void setOldDbUsername(String oldDbUsername) {
        this.oldDbUsername = oldDbUsername;
    }
    public String getOldDbPassword() {
        return oldDbPassword;
    }
    public void setOldDbPassword(String oldDbPassword) {
        this.oldDbPassword = oldDbPassword;
    }
    public String getNewCreateSchemaName() {
        return newCreateSchemaName;
    }
    public void setNewCreateSchemaName(String newCreateSchemaName) {
        this.newCreateSchemaName = newCreateSchemaName;
    }
    public String getOldCreateSchemaName() {
        return oldCreateSchemaName;
    }
    public void setOldCreateSchemaName(String oldCreateSchemaName) {
        this.oldCreateSchemaName = oldCreateSchemaName;
    }
    public String getCommCreateTableName() {
        return commCreateTableName;
    }
    public void setCommCreateTableName(String commCreateTableName) {
        this.commCreateTableName = commCreateTableName;
    }
}

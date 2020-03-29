package jp.co.apasys.table;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import jp.co.apasys.model.MergeTableInfo;

public class MergeTable {
	
	private static String outputBaseFileDirString = "./tables";
	
    private static String newTableNameString = "new.table.name";
    private static String newTableKeyColumn = "new.table.key.column";
    private static String newTableAllColumn = "new.table.all.column";
    private static String newTableMergeColumn = "new.table.merge.column";
    private static String newTableMergeFilter = "new.table.merge.filter";
    private static String oldTableNameString = "old.table.name";
    private static String oldTableKeyColumn = "old.table.key.column";
    private static String oldTableAllColumn = "old.table.all.column";
    private static String oldTableMergeColumn = "old.table.merge.column";
    private static String oldTableMergeFilter = "old.table.merge.filter";
	
	public MergeTable(String mergeTableFile) {
		
	}
	
	public static MergeTableInfo getMergeTableInfo(String mergeTableFile) throws FileNotFoundException, IOException {
		
		MergeTableInfo mergeTableInfo = new MergeTableInfo();
		Properties properties = new Properties();
		properties.load(new FileInputStream(outputBaseFileDirString + File.separator + mergeTableFile));
		
		mergeTableInfo.setNewTableNameString(properties.getProperty(newTableNameString));
		mergeTableInfo.setNewTableKeyColumn(properties.getProperty(newTableKeyColumn));
		mergeTableInfo.setNewTableAllColumn(properties.getProperty(newTableAllColumn));
		mergeTableInfo.setNewTableMergeColumn(properties.getProperty(newTableMergeColumn));
		mergeTableInfo.setNewTableMergeFilter(properties.getProperty(newTableMergeFilter));
		
		mergeTableInfo.setNewTableKeyColumnList(splitToList(properties.getProperty(newTableKeyColumn)));
		mergeTableInfo.setNewTableAllColumnList(splitToList(properties.getProperty(newTableAllColumn)));
		mergeTableInfo.setNewTableMergeColumnList(splitToList(properties.getProperty(newTableMergeColumn)));
		
		mergeTableInfo.setOldTableNameString(properties.getProperty(oldTableNameString));
		mergeTableInfo.setOldTableKeyColumn(properties.getProperty(oldTableKeyColumn));
		mergeTableInfo.setOldTableAllColumn(properties.getProperty(oldTableAllColumn));
		mergeTableInfo.setOldTableMergeColumn(properties.getProperty(oldTableMergeColumn));
		mergeTableInfo.setOldTableMergeFilter(properties.getProperty(oldTableMergeFilter));
		
		mergeTableInfo.setOldTableKeyColumnList(splitToList(properties.getProperty(oldTableKeyColumn)));
		mergeTableInfo.setOldTableAllColumnList(splitToList(properties.getProperty(oldTableAllColumn)));
		mergeTableInfo.setOldTableMergeColumnList(splitToList(properties.getProperty(oldTableMergeColumn)));
		
		return mergeTableInfo;
	}
	
	private static List<String> splitToList(String str) {
		List<String> list = new ArrayList<String>();
		if (str != null && !"".equals(str)) {
			for (String string : str.split(",")) {
				list.add(string.replaceAll(" ", ""));
			}
		}
		return list;
	}
}

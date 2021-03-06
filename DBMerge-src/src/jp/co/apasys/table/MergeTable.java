package jp.co.apasys.table;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import jp.co.apasys.model.MergeTableInfo;

public class MergeTable {

    private static String tablePropertiesPath = "./tables/";

    private static String newTableNameString = "new.table.name";
    private static String newTableKeyColumn = "new.table.key.column";
    private static String newTableMergeColumn = "new.table.merge.column";
    private static String newTableMergeFilter = "new.table.merge.filter";
    private static String oldTableNameString = "old.table.name";
    private static String oldTableKeyColumn = "old.table.key.column";
    private static String oldTableMergeColumn = "old.table.merge.column";
    private static String oldTableMergeFilter = "old.table.merge.filter";

    public MergeTable(String mergeTableFile) {

    }

	public static MergeTableInfo getMergeTableInfo(String mergeTableFile)
	    throws FileNotFoundException, IOException {

	MergeTableInfo mergeTableInfo = new MergeTableInfo();
	Properties properties = new Properties();
	properties.load(new FileInputStream(tablePropertiesPath
		+ mergeTableFile));

	mergeTableInfo.setNewTableNameString(properties
		.getProperty(newTableNameString));
	mergeTableInfo.setNewTableKeyColumn(properties
		.getProperty(newTableKeyColumn));
	mergeTableInfo.setNewTableMergeColumn(properties
		.getProperty(newTableMergeColumn));
	mergeTableInfo.setNewTableMergeFilter(properties
		.getProperty(newTableMergeFilter));

	mergeTableInfo.setNewTableKeyColumnList(splitToList(properties
		.getProperty(newTableKeyColumn)));
	mergeTableInfo.setNewTableMergeColumnList(splitToList(properties
		.getProperty(newTableMergeColumn)));

	mergeTableInfo.setOldTableNameString(properties
		.getProperty(oldTableNameString));
	mergeTableInfo.setOldTableKeyColumn(properties
		.getProperty(oldTableKeyColumn));
	mergeTableInfo.setOldTableMergeColumn(properties
		.getProperty(oldTableMergeColumn));
	mergeTableInfo.setOldTableMergeFilter(properties
		.getProperty(oldTableMergeFilter));

	mergeTableInfo.setOldTableKeyColumnList(splitToList(properties
		.getProperty(oldTableKeyColumn)));
	mergeTableInfo.setOldTableMergeColumnList(splitToList(properties
		.getProperty(oldTableMergeColumn)));

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

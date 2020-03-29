package jp.co.apasys.table;

import java.util.Iterator;
import java.util.List;

import jp.co.apasys.utils.FileUtils;

public class MergeTableCollector {
	private static String outputBaseFileDirString = "./config";
	static Iterator<String> tableIterator = null;
	
	public MergeTableCollector() {
		readMergeTable ();
	}
	
	private void readMergeTable () {
		List<String> mergeTableFileList = FileUtils.readFileAllLines(outputBaseFileDirString);
		tableIterator = mergeTableFileList.iterator();
	}
	
	public static String next() {
		String mergeTableFile = "";
		if (tableIterator.hasNext()) {
			mergeTableFile = tableIterator.next();
		}
		
		return mergeTableFile;
	}
	
	public static boolean hasNext() {
		return tableIterator.hasNext();
	}
}

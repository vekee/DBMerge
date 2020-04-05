package jp.co.apasys.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jp.co.apasys.utils.FileUtils;

public class MergeTableCollector {
	private static String outputBaseFileDirString = "./config/merge.properties";
	private static Iterator<String> tableIterator = null;

	public MergeTableCollector() {
		readMergeTable();
	}

	private void readMergeTable() {
		List<String> mergeTableFileList = new ArrayList<String>();
		try {
			mergeTableFileList = FileUtils.readFileAllLines(outputBaseFileDirString);
		} catch (IOException e) {
			e.printStackTrace();
		}
		tableIterator = mergeTableFileList.iterator();
	}

	public synchronized String next() {
		String mergeTableFile = "";
		if (tableIterator.hasNext()) {
			mergeTableFile = tableIterator.next();
		}

		return mergeTableFile;
	}

}

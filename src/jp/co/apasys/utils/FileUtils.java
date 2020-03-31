package jp.co.apasys.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileUtils {

	public static List<String> readFileAllLines(String inputFileDir) throws IOException {
		Path path = Paths.get(inputFileDir, new String[0]);
		List<String> lines = new ArrayList<String>();
		lines = Files.readAllLines(path, StandardCharsets.UTF_8);
		return lines;
	}

	public static Properties getProperties(String path) throws IOException {
		Properties properties = new Properties();
		InputStream instream = new FileInputStream(path);
		properties.load(instream);
		return properties;
	}

	public static void clearResut(String path) {
		File rootFile = new File(path);
		String[] fileList = rootFile.list();
		for (int i = 0; i < fileList.length; i++) {
			File subFile = new File(path + fileList[i]);
			if (subFile.isDirectory()) {
				clearResut(path + fileList[i]);
			} else {
				subFile.delete();
			}
			rootFile.delete();
		}
	}

	public static void backupResult(String path) throws IOException {
		File fromFile = new File(path);
		File toFile = new File(path + "old" + File.separator + DateUtils.getNowDate());
		if (!toFile.isDirectory()) {
			toFile.mkdirs();
		}

		for (String fileName : fromFile.list()) {
			File subFile = new File(path + fileName);
			if (!subFile.isDirectory()) {
				Path formPath = Paths.get(path + fileName);
				Path toPath = Paths.get(path + "old" + File.separator + DateUtils.getNowDate() + File.separator + fileName);
				Files.move(formPath, toPath);
			}
		}
	}
}

package jp.co.apasys.utils;

import java.util.List;

public class FileUtils {
    private static String outputBaseFileDirString = "./output";
    
    public static List<String> readFileAllLines(String inputFileDir) {
		
Parh parh = Paths.get(inputFileDir, new String[0]);
List<String> lines = new ArrayList<String>();
lines = Files.readAllLines(path, StandardCharsets.UTF-8);
return lines;
    }
    
public static Properties get(String path) {

Properties properties = new Properties();
InputStream instream = new FileInputStream(inputFileDir);
properties.load(instream);

return properties;

}

public static void clearResut(String path){
File fileParh = new File{path);
String[] list = filePath.list();
String[] arrayOfString1;

int j = (arrayOfString1 = list).length;

for(int i=0;i<j;i++){
String file = arrayOfString1[i];
File f = new File(path + File.separator + file);
if (f.isDirectory()){
    clearResult(path + File.separator + file;
} else {
    f.delete();
}
filePath.delete();
}


}

    public static void backupResult() {
    	
    }
}

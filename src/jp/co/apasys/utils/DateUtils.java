package jp.co.apasys.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

	public static String getNowDate() {
		Calendar calendar = Calendar.getInstance();
		Date date = calendar.getTime();
		SimpleDateFormat sdformat = new SimpleDateFormat("yyyyMMddHHmmss");
		return sdformat.format(date);
	}

}

package com.wanda.ffan.common.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by liuzhenfeng on 16/3/17.
 */
public class NumberUtils {

	public static long parseLong(String str) {
		try {
			return Long.parseLong(StringUtils.trimToEmpty(str));
		} catch (Exception e) {
		}
		return -1l;
	}

	public static int parseInt(String str) {
		try {
			return Integer.parseInt(StringUtils.trimToEmpty(str));
		} catch (Exception e) {
		}
		return -1;
	}

	/**
	 * 判断是否4开头
	 * 
	 * @param value
	 * @return
	 */
	public static boolean judge4xx(int value) {
		String str = String.valueOf(value);
		int head = str.charAt(0) - 48;//'0'
		if (head == 4) {
			return true;
		}
		return false;
	}
}

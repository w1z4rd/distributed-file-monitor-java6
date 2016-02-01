package org.costa;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DFMProperties {
	private static Properties properties = new Properties();

	static {
		InputStream inputStream = null;
		try {
			String propertiesFileName = "config.properties";
			inputStream = new FileInputStream(propertiesFileName);
			properties.load(inputStream);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
	}

	private DFMProperties() {

	}

	public static String getProperty(String name) {
		return properties.getProperty(name);
	}
	
	public static long getLongProperty(String name, long defaultValue) {
		String propertiy = properties.getProperty(name, String.valueOf(defaultValue));
		return Long.parseLong(propertiy);
	}

}

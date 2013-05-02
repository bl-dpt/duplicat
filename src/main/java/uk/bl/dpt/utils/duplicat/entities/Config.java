package uk.bl.dpt.utils.duplicat.entities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Config {
	public static final String PATHS = "paths";
	public static final String DBENV = "db_env";
	public static final String STORE = "store";
	public static final String NUMTHREADS = "num_threads";
	public static final String THREADLOAD = "thread_load_wait";

	public static final String THREADLOAD_DEFAULT = "5000";
	public static final String NUMTHREADS_DEFAULT = "4";

	private Properties props;

	@SuppressWarnings("unused")
	private Config() {
		// must provide proplocation so hide default constructor
	}

	public Config(String propLocation) throws IOException {
		File pfile = new File(propLocation);
		BufferedReader r = new BufferedReader(new FileReader(pfile));
		props = new Properties();
		props.load(r);
	}

	public String[] getPaths() {
		String paths = props.getProperty(PATHS);
		return paths.split(",");
	}

	public String getDBEnv() {
		return props.getProperty(DBENV);
	}

	public String getStoreName() {
		String defaultStoreName = "duplicat";
		return props.getProperty(STORE, defaultStoreName);
	}

	public int getThreadWait() {
		String numStr = props.getProperty(THREADLOAD, THREADLOAD_DEFAULT);
		int num;
		try {
			num = Integer.parseInt(numStr);
		} catch (NumberFormatException e) {
			num = Integer.parseInt(THREADLOAD_DEFAULT);
		}
		return num;
	}

	public int getNumThreads() {
		String numStr = props.getProperty(NUMTHREADS, NUMTHREADS_DEFAULT);
		int num;
		try {
			num = Integer.parseInt(numStr);
		} catch (NumberFormatException e) {
			num = Integer.parseInt(NUMTHREADS_DEFAULT);
		}
		return num;
	}
}

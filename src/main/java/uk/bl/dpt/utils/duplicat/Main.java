package uk.bl.dpt.utils.duplicat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class Main {

	private Environment env = null;
	private EntityStore store = null;
	private FilePathDA pathDA = null;
	private FileEntityDA fileDA = null;

	private String dbenvPath;
	private String storeName;
	private List<String> rootPaths = null;

	private long startTime;
	private long endTime;
	private int numThreads = 8;

	private ExecutorService threadPool;

	public static void main(String[] args) {
		Options opts = new Options();
		Option cOpt = new Option("c", true,
				"The location of the configuration properties file");
		cOpt.setRequired(true);
		opts.addOption(cOpt);
		opts.addOption("l", false,
				"Lists the content of the dbenv/store given in conf file");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(opts, args);
		} catch (MissingOptionException e) {
			System.out.println("You must specify a config file using -c FILE");
			System.exit(-1);
		} catch (ParseException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		Main mi = new Main();
		if (cmd.hasOption('l')) {
			mi.list(cmd.getOptionValue('c'));
		} else {
			mi.go(cmd.getOptionValue('c'));
		}
	}

	private void list(String confPath) {
		init(confPath);
		System.out.println(" FilePathEntities Listing ");
		System.out.println(" ======================== ");
		DBUtils.listIdx(fileDA.getPrimaryIndex());
	}

	public void init(String confPath) {
		Config conf = null;
		try {
			conf = new Config(confPath);
		} catch (IOException e) {
			doErrorAndExit("Unable to read config file: " + confPath);
		}
		if (conf != null) {
			rootPaths = Arrays.asList(conf.getPaths());
			dbenvPath = conf.getDBEnv();
			File dbef = new File(dbenvPath);
			dbef.mkdirs();
			storeName = conf.getStoreName();
			numThreads = conf.getNumThreads();
			threadPool = Executors.newFixedThreadPool(numThreads);
			try {
				initDb();
			} catch (DatabaseException e) {
				doErrorAndExit("Unable to initialise database: "
						+ e.getMessage());
			}
		} else {
			doErrorAndExit("Config was null.");
		}
	}

	private void doErrorAndExit(String msg) {
		try {
			closeDb();
		} catch (DatabaseException e) {
			// nevermind
		}
		System.out.println("\nError:\n" + msg + "\n\n");
		System.exit(-1);
	}

	public void initDb() throws DatabaseException {
		EnvironmentConfig envConf = new EnvironmentConfig();
		envConf.setAllowCreate(true);

		StoreConfig storeConf = new StoreConfig();
		storeConf.setAllowCreate(true);

		env = new Environment(new File(dbenvPath), envConf);
		store = new EntityStore(env, storeName, storeConf);

		pathDA = new FilePathDA(store);
		fileDA = new FileEntityDA(store);
	}

	public void closeDb() throws DatabaseException {
		if (store != null) {
			store.close();
		}
		if (env != null) {
			env.close();
		}
	}

	public void go(String confPath) {
		startTime = System.currentTimeMillis();

		init(confPath);

		System.out.println("Clearing files from DB...");
		DBUtils.purgeIdx(pathDA.getPrimaryIndex());
		DBUtils.purgeIdx(fileDA.getPrimaryIndex());
		System.out.println(" Initial DB Counts: P: "
				+ DBUtils.getCountIdx(pathDA.getPrimaryIndex()) + " F: "
				+ DBUtils.getCountIdx(fileDA.getPrimaryIndex()));

		for (String path : rootPaths) {
			File root = new File(path);
			if (root.exists()) {
				storeFilePathsRecursive(root, path);
			} else {
				System.out.println(" Skipping: " + path + ": DOES NOT EXIST");
			}
		}

		System.out.println(" Found DB Count: "
				+ DBUtils.getCountIdx(pathDA.getPrimaryIndex()));

		PrimaryIndex<Path, FilePathEntity> idx = pathDA.getPrimaryIndex();

		try {
			EntityCursor<FilePathEntity> ec = idx.entities();
			FilePathEntity fpe;
			while ((fpe = ec.next()) != null) {
				FileInfoExtractThread fred = new FileInfoExtractThread(fpe,
						new FileEntityDA(store));
				threadPool.execute(fred);
			}
			ec.close();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

		threadPool.shutdown();

		System.out.print(" ");

		int i = 0;

		while (!threadPool.isTerminated()) {
			try {
				Thread.sleep(100);
				if (i == 80) {
					System.out.print("\n ");
					i = 0;
				} else {
					System.out.print(".");
					i++;
				}
			} catch (InterruptedException ie) {
				// don't really care! :-)
			}
		}

		System.out.print("\n");

		System.out.println(" Final DB Counts: P: "
				+ DBUtils.getCountIdx(pathDA.getPrimaryIndex()) + " F: "
				+ DBUtils.getCountIdx(fileDA.getPrimaryIndex()));

		try {
			closeDb();
		} catch (DatabaseException dbe) {
			dbe.printStackTrace();
			System.exit(-1);
		}

		endTime = System.currentTimeMillis();
		float timeTaken = (endTime - startTime) / 1000;
		System.out.printf("Total execution time: %f secs\n", timeTaken);
	}

	private void storeFilePathsRecursive(File f, String source) {
		if (f.isDirectory()) {
			File[] files = f.listFiles();
			for (File nextfile : files) {
				storeFilePathsRecursive(nextfile, source);
			}
		} else { // Assume we found a file!
			Path path = new Path();
			path.path = f.getAbsolutePath();
			path.source = source;
			FilePathEntity ent = new FilePathEntity(path);
			try {
				pathDA.put(ent);
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
	}
}

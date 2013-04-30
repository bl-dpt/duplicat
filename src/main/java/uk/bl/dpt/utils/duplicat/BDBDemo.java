package uk.bl.dpt.utils.duplicat;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

public class BDBDemo {

	private Environment env = null;
	private EntityStore store = null;
	private FileEntityDA feDA = null;

	private String dbenvPath = "./dbEnv/";
	private String storeName = "duplicat";
	private List<String> rootPaths = null;

	private long startTime;
	private long endTime;
	private String startPath = "C:/Users/pcliff/Pictures/Background";

	public static void main(String[] args) {
		BDBDemo mi = new BDBDemo();
		mi.go();
	}

	public void initDb() throws DatabaseException {
		EnvironmentConfig envConf = new EnvironmentConfig();
		envConf.setAllowCreate(true);

		StoreConfig storeConf = new StoreConfig();
		storeConf.setAllowCreate(true);

		env = new Environment(new File(dbenvPath), envConf);
		store = new EntityStore(env, storeName, storeConf);

		feDA = new FileEntityDA(store);
	}

	public void closeDb() throws DatabaseException {
		if (store != null) {
			store.close();
		}
		if (env != null) {
			env.close();
		}
	}

	public void go() {
		rootPaths = new ArrayList<String>();
		rootPaths.add(startPath );
		startTime = System.currentTimeMillis();

		try {
			initDb();
		} catch (DatabaseException dbe) {
			dbe.printStackTrace();
			System.exit(-1);
		}

		System.out.println("Clearing files from DB...");
		DBUtils.purgeIdx(feDA.getPrimaryIndex());
		System.out.println(" Initial DB Count: "
				+ DBUtils.getCountIdx(feDA.getPrimaryIndex()));

		for (String path : rootPaths) {
			File root = new File(path);
			if (root.exists()) {
				walkFileTree(root);
			} else {
				System.out.println(" Skipping: " + path + ": DOES NOT EXIST");
			}
		}

		DBUtils.listIdx(feDA.getPrimaryIndex());

		System.out.println(" Final DB Count: "
				+ DBUtils.getCountIdx(feDA.getPrimaryIndex()));

		try {
			closeDb();
		} catch (DatabaseException dbe) {
			dbe.printStackTrace();
			System.exit(-1);
		}
		
		endTime = System.currentTimeMillis();
		long timeTaken = (endTime - startTime)/1000;
		System.out.println("Total execution time: " + timeTaken + " secs");
	}

	private void walkFileTree(File f) {
		if (f.isDirectory()) {
			File[] files = f.listFiles();
			for (File nextfile : files) {
				walkFileTree(nextfile);
			}
		} else { // Assume we found a file!
			String path = f.getAbsolutePath();
			FileEntity ent = new FileEntity(path);
			ent.addPath(path);
			try {
				feDA.put(ent);
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
	}
}

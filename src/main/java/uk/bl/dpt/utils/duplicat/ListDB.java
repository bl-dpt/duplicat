package uk.bl.dpt.utils.duplicat;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

public class ListDB {

	private Environment env;
	private String dbenvPath = "C:/BLWork/DigiPres1/dbenv";
	private EntityStore store;
	private String storeName = "duplicat";
	
	public static void main(String[] args) {
		ListDB mi = new ListDB();
		try {
			mi.go();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
	
	public void go() throws DatabaseException {
		initDb();
		FileEntityDA feda = new FileEntityDA(store);
		DBUtils.listIdx(feda.getPrimaryIndex());
		closeDb();
	}

	public void initDb() throws DatabaseException {
		EnvironmentConfig envConf = new EnvironmentConfig();
		envConf.setAllowCreate(true);

		StoreConfig storeConf = new StoreConfig();
		storeConf.setAllowCreate(true);

		env = new Environment(new File(dbenvPath), envConf);
		store = new EntityStore(env, storeName, storeConf);
	}

	public void closeDb() throws DatabaseException {
		if (store != null) {
			store.close();
		}
		if (env != null) {
			env.close();
		}
	}

}

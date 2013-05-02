package uk.bl.dpt.utils.duplicat.filelist;

import java.io.File;

import uk.bl.dpt.utils.duplicat.entities.Config;
import uk.bl.dpt.utils.duplicat.entities.FilePathEntity;
import uk.bl.dpt.utils.duplicat.entities.Path;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class FileListDA {

	private Environment env;
	private EntityStore store;
	private PrimaryIndex<Path, FilePathEntity> pidx;

	@SuppressWarnings("unused")
	private FileListDA() {
		// Can't see me!
	}

	public FileListDA(Config conf) throws DatabaseException {
		initDb(conf);
		pidx = store.getPrimaryIndex(Path.class, FilePathEntity.class);
	}

	private void initDb(Config conf) throws DatabaseException {
		EnvironmentConfig envConf = new EnvironmentConfig();
		envConf.setAllowCreate(true);

		StoreConfig storeConf = new StoreConfig();
		storeConf.setAllowCreate(true);

		String envPath = conf.getDBEnv();
		String storeName = conf.getStoreName();

		env = new Environment(new File(envPath), envConf);
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

	public void put(FilePathEntity newEntity) throws DatabaseException {
		if (pidx != null) {
			pidx.put(newEntity);
			//TODO - shouldn't really happen, but do we care if we end up overwriting listings?
			// if (pidx.contains(ent.getPK())) {
			// throw new DatabaseException("Cannot insert duplicate value!");
			// } else {
			// pidx.put(ent);
			// }
		} else {
			throw new DatabaseException("Cannot put to a null index!");
		}
	}

	public FilePathEntity get(Path key) throws DatabaseException {
		if (pidx != null) {
			return pidx.get(key);
		} else {
			throw new DatabaseException("Cannot get from a null index!");
		}
	}

	public void close() {
		try {
			closeDb();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
	
	public void purge() throws DatabaseException {
		if (pidx != null) {
			EntityCursor<?> ec = pidx.entities();
			while (ec.next() != null) {
				ec.delete();
			}
			ec.close();
		} else {
			throw new DatabaseException("Cannot purge null index!");
		}
	}
	
	public long count() throws DatabaseException {
		if ( pidx != null ) {
			return pidx.count();
		} else {
			throw new DatabaseException("Cannot count from a null index!");
		}
	}
	
	public EntityCursor<FilePathEntity> getEntities() throws DatabaseException {
		if ( pidx != null ) {
			return pidx.entities();
		} else {
			throw new DatabaseException("Cannot get entites from a null index!");
		}
	}
}

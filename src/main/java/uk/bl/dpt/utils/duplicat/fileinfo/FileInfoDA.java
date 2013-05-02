package uk.bl.dpt.utils.duplicat.fileinfo;

import java.io.File;
import java.util.List;

import uk.bl.dpt.utils.duplicat.entities.Config;
import uk.bl.dpt.utils.duplicat.entities.FileInfoEntity;
import uk.bl.dpt.utils.duplicat.entities.Path;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class FileInfoDA {

	private Environment env;
	private EntityStore store;
	private PrimaryIndex<String, FileInfoEntity> pidx = null;

	@SuppressWarnings("unused")
	private FileInfoDA() {
		// Can't see me!
	}

	public FileInfoDA(Config conf) throws DatabaseException {
		initDb(conf);
		pidx = store.getPrimaryIndex(String.class, FileInfoEntity.class);
	}

	private void initDb(Config conf) throws DatabaseException {
		EnvironmentConfig envConf = new EnvironmentConfig();
		envConf.setAllowCreate(true);

		StoreConfig storeConf = new StoreConfig();
		storeConf.setAllowCreate(true);

		env = new Environment(new File(conf.getDBEnv()), envConf);
		store = new EntityStore(env, conf.getStoreName(), storeConf);
	}

	private void closeDb() throws DatabaseException {
		if (store != null) {
			store.close();
		}
		if (env != null) {
			env.close();
		}
	}

	/**
	 * Adds an entity if it doesn't exist (PK is SHA256) or updates an existing
	 * entity with the paths of the new one. All other fields in the new one are
	 * ignored.
	 * 
	 * @param newEntity
	 *            The entity to add/append paths of
	 * @throws DatabaseException
	 */
	public void append(FileInfoEntity newEntity) throws DatabaseException {
		if (pidx != null) {
			if (pidx.contains(newEntity.getId())) {
				FileInfoEntity existingEntity = pidx.get(newEntity.getId());
				List<Path> paths = newEntity.getPaths();
				if (paths != null) {
					for (Path path : paths) {
						existingEntity.addPath(path);
					}
				}
				// This put will update the existing object. Only works
				// if the primary key is unchanged.
				pidx.put(existingEntity);
			} else {
				// New item, just add it as is.
				pidx.put(newEntity);
			}
		} else {
			throw new DatabaseException("Cannot append to a null index!");
		}
	}

	public FileInfoEntity put(FileInfoEntity newEntity)
			throws DatabaseException {
		if (pidx != null) {
			return pidx.put(newEntity);
		} else {
			throw new DatabaseException("Cannot put from a null index!");
		}
	}

	public FileInfoEntity get(String key) throws DatabaseException {
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
	
	public EntityCursor<FileInfoEntity> getEntities() throws DatabaseException {
		if ( pidx != null ) {
			return pidx.entities();
		} else {
			throw new DatabaseException("Cannot get entites from a null index!");
		}
	}
}

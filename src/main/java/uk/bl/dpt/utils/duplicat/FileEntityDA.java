package uk.bl.dpt.utils.duplicat;

import java.util.Iterator;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

public class FileEntityDA {
	private PrimaryIndex<String, FileEntity> pidx = null;

	public FileEntityDA(EntityStore store) throws DatabaseException {
		pidx = store.getPrimaryIndex(String.class, FileEntity.class);
	}

	/**
	 * Put here is a bit special as it takes care of paths. If an entity exists
	 * in the database then the paths of the input parameter are appended to the
	 * existing object. The rest of the input entity is discarded.
	 * 
	 * @param newEntity
	 *            A FileEntity representing the paths to add.
	 * @throws DatabaseException
	 *             If it is closed, etc.
	 */
	public void put(FileEntity newEntity) throws DatabaseException {
		if (pidx != null) {
			if (pidx.contains(newEntity.getId())) {
				FileEntity existingEntity = pidx.get(newEntity.getId());
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
			throw new DatabaseException("Cannot put to a null index!");
		}
	}

	public FileEntity get(String idx) throws DatabaseException {
		if (pidx != null) {
			return pidx.get(idx);
		} else {
			throw new DatabaseException("Cannot get from a null index!");
		}
	}

	public Iterator<FileEntity> getEntities() throws DatabaseException {
		if (pidx != null) {
			return pidx.entities().iterator();
		} else {
			throw new DatabaseException(
					"Cannot get entities from a null index!");
		}
	}

	public PrimaryIndex<String, FileEntity> getPrimaryIndex() {
		return pidx;
	}
}

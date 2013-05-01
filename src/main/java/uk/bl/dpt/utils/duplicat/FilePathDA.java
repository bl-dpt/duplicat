package uk.bl.dpt.utils.duplicat;

import java.util.Iterator;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

public class FilePathDA {
	private PrimaryIndex<Path, FilePathEntity> pidx = null;
	
	public FilePathDA(EntityStore store) throws DatabaseException {
		pidx = store.getPrimaryIndex(Path.class, FilePathEntity.class);
	}
	
	public void put(FilePathEntity ent) throws DatabaseException {
		if (pidx != null) {
			if (pidx.contains(ent.getPK())){
				throw new DatabaseException("Cannot insert duplicate value!");
			} else {
				pidx.put(ent);
			}
		} else {
			throw new DatabaseException("Cannot put to a null index!");
		}
	}
	
	public FilePathEntity get(Path idx) throws DatabaseException {
		if (pidx != null) {
			return pidx.get(idx);
		} else {
			throw new DatabaseException("Cannot get from a null index!");
		}
	}
	
	public Iterator<FilePathEntity> getEntities() throws DatabaseException {
		if ( pidx != null ) {
			return pidx.entities().iterator();
		} else {
			throw new DatabaseException("Cannot get entities from a null index!");
		}	
	}
	
	public PrimaryIndex<Path, FilePathEntity> getPrimaryIndex() {
		return pidx;
	}
}

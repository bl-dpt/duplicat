package uk.bl.dpt.utils.duplicat;

import java.util.Iterator;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

public class FileEntityDA {
	private PrimaryIndex<String, FileEntity> pidx = null;
	
	public FileEntityDA(EntityStore store) throws DatabaseException {
		pidx = store.getPrimaryIndex(String.class, FileEntity.class);
	}
	
	public void put(FileEntity ent) throws DatabaseException {
		if (pidx != null) {
			if (pidx.contains(ent.getId())){
				throw new DatabaseException("Cannot insert duplicate value!");
			} else {
				pidx.put(ent);
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
		if ( pidx != null ) {
			return pidx.entities().iterator();
		} else {
			throw new DatabaseException("Cannot get entities from a null index!");
		}	
	}
	
	public PrimaryIndex<String, FileEntity> getPrimaryIndex() {
		return pidx;
	}
}

package uk.bl.dpt.utils.duplicat.entities;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class FilePathEntity {
	@PrimaryKey
	private Path path;
	
	public FilePathEntity() {
		
	}

	public FilePathEntity(Path path) {
		this.path = path;
	}

	public Path getPath() {
		return this.path;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public Path getPK() {
		return this.path;
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + ": " + path.toString();
	}
}

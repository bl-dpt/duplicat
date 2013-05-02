package uk.bl.dpt.utils.duplicat.entities;

import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;

@Persistent
public class Path {
	@KeyField(1)
	public String path;
	@KeyField(2)
	public String source;
	public String toString() {
		return path + " [" + source + "]";
	}
}

package uk.bl.dpt.utils.duplicat.entities;

import java.util.ArrayList;
import java.util.List;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class FileInfoEntity {
	@PrimaryKey
	public String id;
	public List<Path> paths = null;
	public String sha256 = null;
	public String type = null;
	
	public FileInfoEntity() {
		
	}
	
	public FileInfoEntity(String id) {
		this.id = id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getId() {
		return this.id;
	}
	
	public void setSha256(String sha256) {
		this.sha256 = sha256;
	}
	
	public String getSha256() {
		return this.sha256;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public String getType() {
		return this.type;
	}
	
	public void addPath(Path path) {
		if (paths == null) {
			paths = new ArrayList<Path>();
		}
		paths.add(path);
	}
	
	public List<Path> getPaths() {
		return this.paths;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("--digipresInfo--\n");
		sb.append("SHA256: " + sha256 + "\n");
		sb.append("PATHS\n");
		for(Path p : paths) {
			sb.append(p+"\n");
		}
		sb.append("----------------\n");
		return sb.toString();
	}
	
	public String toXML() {
		StringBuffer sb = new StringBuffer();
		sb.append("<?xml version=\"1.0\" ?>\n<digipresInfo>\n");
		sb.append("  <sha256> " + sha256 + " </sha256>\n");
		sb.append("  <type> " + type + " </type>\n");
		sb.append("  <paths>\n");
		for(Path p : paths) {
			sb.append("    <path> " + p + " </path>\n");
		}
		sb.append("  </paths>\n");
		sb.append("</digipresInfo>\n");
		return sb.toString();
	}}

package uk.bl.dpt.utils.duplicat;

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.DatabaseException;

public class FileInfoExtractThread extends Thread {
	private File file;
	private FileEntityDA da;
	private FilePathEntity fpe;
	
	@SuppressWarnings("unused")
	private FileInfoExtractThread() {
		// No default constructor!
	}
	
	public FileInfoExtractThread(FilePathEntity fpe, FileEntityDA da) {
		this.file = new File(fpe.getPath().path);
		this.fpe = fpe;
		this.da = da; // TODO is this the best way to get the data into the DB??
	}
	
	@Override
	public void run() {
		SHA256 cookieMonster = new SHA256();
		String sha256;
		try {
			sha256 = cookieMonster.digest(file);
		} catch (IOException e) {
			sha256 = "UNKNOWN_SHA256";
		}
		FileEntity ent = new FileEntity();
		ent.setId(sha256);
		ent.setSha256(sha256);
		Path p = fpe.getPath();
		ent.addPath(p);
		try {
			da.put(ent);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

}

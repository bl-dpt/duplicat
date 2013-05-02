package uk.bl.dpt.utils.duplicat.processing.threaded;

import java.io.File;
import java.io.IOException;

import uk.bl.dpt.utils.duplicat.entities.FileInfoEntity;
import uk.bl.dpt.utils.duplicat.entities.FilePathEntity;
import uk.bl.dpt.utils.duplicat.entities.Path;
import uk.bl.dpt.utils.duplicat.fileinfo.FileInfoDA;
import uk.bl.dpt.utils.duplicat.util.SHA256;

import com.sleepycat.je.DatabaseException;

public class FileInfoExtractThread extends Thread {
	private static final String UNKNOWN_SHA256 = "UNKNOWN_SHA256";
	
	private File file;
	private FileInfoDA da;
	private FilePathEntity fpe;
	
	@SuppressWarnings("unused")
	private FileInfoExtractThread() {
		// No default constructor!
	}
	
	public FileInfoExtractThread(FilePathEntity fpe, FileInfoDA da) {
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
		} catch (IOException e) { //TODO Catch out of mem here?
			sha256 = UNKNOWN_SHA256;
		}
		FileInfoEntity ent = new FileInfoEntity();
		ent.setId(sha256);
		ent.setSha256(sha256);
		Path p = fpe.getPath();
		ent.addPath(p);
		try {
			da.append(ent); //nb. NOT put
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		sha256 = null;
		cookieMonster = null;
		file = null;
		fpe = null;
		da = null;
		p = null;
		ent = null;
	}
}

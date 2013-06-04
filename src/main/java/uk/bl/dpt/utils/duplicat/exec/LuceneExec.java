package uk.bl.dpt.utils.duplicat.exec;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

public class LuceneExec {

	public static final String FIELD_PATH = "duplicat_path";
	public static final String FIELD_SOURCE = "duplicat_source";
	public static final String FIELD_SHA256 = "duplicat_sha256";
	public static final String FIELD_TYPE = "duplicat_type";

	private Config conf;

	public LuceneExec(final Config conf) {
		this.conf = conf;
	}

	public void purgeFileList() {
		System.out.println(" Deleting file list index...");
		try {
			LuceneDA.purgeIndex(LuceneDA.IDX.FILELIST, conf.getDBEnv());
		} catch (IOException e) {
			// TODO LOG
			e.printStackTrace();
		}
	}

	public void purgeFileInfo() {
		System.out.println(" Deleting file info index...");
		try {
			LuceneDA.purgeIndex(LuceneDA.IDX.FILEINFO, conf.getDBEnv());
		} catch (IOException e) {
			// TODO LOG
			e.printStackTrace();
		}
	}

	public void purgeDatabases() {
		purgeFileList();
		purgeFileInfo();
	}

	public void listDatabase() {
		System.out.println("Lucene:List");
		System.out.println("===========");
		System.out.println("File List:");
		IndexReader reader = null;
		try {
			reader = LuceneDA.getIndexReader(LuceneDA.IDX.FILELIST,
					conf.getDBEnv());
			int totalDocs = reader.numDocs();
			for (int i = 0; i < totalDocs; i++) {
				if (!reader.isDeleted(i)) {
					Document doc = reader.document(i);
					System.out.println(doc.toString());
				}
			}
		} catch (IOException e) {
			System.out.println("Unable to read index: " + e.getMessage());
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("\n\nFile Info:");
		try {
			reader = LuceneDA.getIndexReader(LuceneDA.IDX.FILEINFO,
					conf.getDBEnv());
			int totalDocs = reader.numDocs();
			for (int i = 0; i < totalDocs; i++) {
				if (!reader.isDeleted(i)) {
					Document doc = reader.document(i);
					System.out.println(doc.toString());
				}
			}
		} catch (IOException e) {
			System.out.println("Unable to read index: " + e.getMessage());
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
					// Don't really care though...
				}
			}
		}
	}

	public void createFileInfo() {
		System.out.print("Getting file info");
		FileInfoExtract fie;
		fie = new FileInfoExtract(conf);
		fie.go();
	}

	public void setConfig(Config conf) {
		this.conf = conf;
	}

	public Config getConfig() {
		return conf;
	}

	public void open() {
	}

	public void close() {
	}

	public void createFileList() {
		System.out.println("Creating file list");
		FileLister fileLister = new FileLister(conf);
		fileLister.createFileList();
	}
}

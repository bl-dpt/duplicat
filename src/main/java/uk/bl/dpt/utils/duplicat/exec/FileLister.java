package uk.bl.dpt.utils.duplicat.exec;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;


public class FileLister {
	Config conf;
	
	public FileLister(Config conf) {
		this.conf = conf;
	}

	public void createFileList() {
		System.out.print("File listing: ");
		IndexWriter flwriter = null;
		try {
			flwriter = LuceneDA.getIndexWriter(LuceneDA.IDX.FILELIST, conf.getDBEnv());
			for (String path : conf.getPaths()) {
				storeFilePathsRecursive(flwriter, new File(path), path);
				// TODO see if that gets too big passing the writer about.
			}
		} catch (IOException e) {
			System.out.println("createFileList: Unable to get indexWriter: " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				flwriter.commit();
				flwriter.close();
			} catch (IOException e) {
				// TODO might want to catch the commit separately??
			}
		}
		System.out.println(" Complete.");

		IndexReader ir = null;
		try {
			ir = LuceneDA.getIndexReader(LuceneDA.IDX.FILELIST, conf.getDBEnv());
			System.out.println(" Found: " + ir.numDocs() + " items to process");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				ir.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void storeFilePathsRecursive(IndexWriter flwriter, File f,
			String source) {
		if (f.isDirectory()) {
			File[] files = f.listFiles();
			if (files != null) {
				for (File nextfile : files) {
					storeFilePathsRecursive(flwriter, nextfile, source);
				}
			} else {
				System.out.println("No files found in " + f.getAbsolutePath());
			}
		} else { // Assume we found a file!
			Document doc = new Document();
			doc.add(new Field(LuceneDA.FIELD_PATH, f.getAbsolutePath(), Field.Store.YES,
					Field.Index.NOT_ANALYZED));
			doc.add(new Field(LuceneDA.FIELD_SOURCE, source, Field.Store.YES,
					Field.Index.NOT_ANALYZED));
			try {
				flwriter.addDocument(doc);
			} catch (IOException e) {
				System.out.println("Unable to store: " + f.getAbsolutePath());
				e.printStackTrace();
			}
		}
	}

}

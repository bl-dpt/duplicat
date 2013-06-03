package uk.bl.dpt.utils.duplicat.exec;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class LuceneDA {
	public static final String FIELD_PATH = "path";
	public static final String FIELD_SHA256 = "sha256";
	public static final String FIELD_TYPE = "type";
	public static final String FIELD_SOURCE = "source";
	public static final String FIELD_SRCSHA = "srcsha256";


	public enum IDX {
		FILELIST, FILEINFO;
	}

	public static final String LUCENE_IDX_PRE = "lucene_idx_";
	public static final String FL_LUCENE_IDX = LUCENE_IDX_PRE + "flist";
	public static final String FI_LUCENE_IDX = LUCENE_IDX_PRE + "finfo";

	public static IndexSearcher getIndexSearcher(IDX idx, String dbEnvPath)
			throws IOException {
		IndexReader reader = getIndexReader(idx, dbEnvPath);
		return new IndexSearcher(reader);
	}
	
	public static IndexReader getIndexReader(IDX idx, String dbEnvPath)
			throws IOException {
		Directory dir = null;
		switch (idx) {
		case FILELIST:
			dir = FSDirectory.open(new File(dbEnvPath + "/" + FL_LUCENE_IDX));
			break;
		case FILEINFO:
			dir = FSDirectory.open(new File(dbEnvPath + "/" + FI_LUCENE_IDX));
			break;
		default:
			return null;
		}
		return IndexReader.open(dir);
	}

	public static IndexWriter getIndexWriter(IDX idx, String dbEnvPath) throws IOException {
		Directory dir = null;
		switch (idx) {
		case FILELIST:
			dir = FSDirectory.open(new File(dbEnvPath + "/" + FL_LUCENE_IDX));
			break;
		case FILEINFO:
			dir = FSDirectory.open(new File(dbEnvPath + "/" + FI_LUCENE_IDX));
			break;
		default:
			return null;
		}
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
		IndexWriterConfig writerConfig = new IndexWriterConfig(
				Version.LUCENE_36, analyzer);
		return new IndexWriter(dir, writerConfig); //TODO this throws other exceptions - we bury them all in IOException. Does it matter?
	}

	public static void purgeIndex(IDX idx, String dbEnvPath)
			throws IOException {
		IndexWriter writer = getIndexWriter(idx, dbEnvPath);
		writer.deleteAll();
		writer.commit();
		writer.close();
	}
}

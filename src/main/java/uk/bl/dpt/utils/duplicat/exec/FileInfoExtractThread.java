package uk.bl.dpt.utils.duplicat.exec;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;

import uk.bl.dpt.utils.duplicat.util.SHA256;

public class FileInfoExtractThread extends Thread {
	private static final String UNKNOWN_SHA256 = "UNKNOWN_SHA256";

	private IndexWriter fiwriter;
	private Document fileListDoc;

	@SuppressWarnings("unused")
	private FileInfoExtractThread() {
		// No default constructor!
	}

	public FileInfoExtractThread(Document fileListDoc, IndexWriter fiwriter) {
		this.fileListDoc = fileListDoc;
		this.fiwriter = fiwriter; // TODO is this the best way to get the
	}

	@Override
	public void run() {
		File file = new File(fileListDoc.get(LuceneDA.FIELD_PATH));
		SHA256 cookieMonster = new SHA256();
		String sha256 = null;
		String srcsha256 = null;
		try {
			sha256 = cookieMonster.digest(file);
			String srcsha = fileListDoc.get(LuceneDA.FIELD_SOURCE) + sha256;
			srcsha256 = cookieMonster.digest(
					new ByteArrayInputStream(srcsha.getBytes()), true);
		} catch (IOException e) { // TODO Catch out of mem here?
			sha256 = UNKNOWN_SHA256;
		}
		Document fileInfoDoc = new Document();
		fileInfoDoc.add(new Field(LuceneDA.FIELD_PATH, file.getAbsolutePath(),
				Field.Store.YES, Field.Index.NOT_ANALYZED));
		fileInfoDoc.add(new Field(LuceneDA.FIELD_SOURCE, fileListDoc
				.get(LuceneDA.FIELD_SOURCE), Field.Store.YES,
				Field.Index.NOT_ANALYZED));
		fileInfoDoc.add(new Field(LuceneDA.FIELD_SHA256, sha256,
				Field.Store.YES, Field.Index.NOT_ANALYZED));
		fileInfoDoc.add(new Field(LuceneDA.FIELD_SRCSHA, srcsha256,
				Field.Store.YES, Field.Index.NOT_ANALYZED));
		try {
			fiwriter.addDocument(fileInfoDoc);
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

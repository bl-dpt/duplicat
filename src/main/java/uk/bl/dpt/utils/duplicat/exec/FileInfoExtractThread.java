package uk.bl.dpt.utils.duplicat.exec;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;

import uk.bl.dpt.utils.duplicat.util.SHA256;

public class FileInfoExtractThread extends Thread {
	private static final String UNKNOWN_SHA256 = "UNKNOWN_SHA256";
	private static final String UNKNOWN_DATE = "1900-01-01";
	private static final String UNKNOWN_TYPE = "type/unknown";
	
	private IndexWriter fiwriter;
	private Document fileListDoc;
	private Tika tika;

	@SuppressWarnings("unused")
	private FileInfoExtractThread() {
		// No default constructor!
	}

	public FileInfoExtractThread(Document fileListDoc, IndexWriter fiwriter) {
		this.fileListDoc = fileListDoc;
		this.fiwriter = fiwriter; // TODO is this the best way to get the
		this.tika = new Tika();
	}

	@Override
	public void run() {
		File file = new File(fileListDoc.get(LuceneDA.FIELD_PATH));
		SHA256 cookieMonster = new SHA256();
		String sha256 = null;
		String srcsha256 = null;
		String type = null;
		String date = null;
		try {
			sha256 = cookieMonster.digest(file);
			type = getType(file);
			date = getDate(file);
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
		fileInfoDoc.add(new Field(LuceneDA.FIELD_TYPE, type,
				Field.Store.YES, Field.Index.NOT_ANALYZED));
		fileInfoDoc.add(new Field(LuceneDA.FIELD_DATE, date,
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
	
	private String getType(File f) {
		String type = null;
		try {
			type = tika.detect(f);
		} catch (IOException e) {
			System.err.println("Failed to detect: " + f.getAbsolutePath());
			return UNKNOWN_TYPE;
		}
		return type;
	}
	
	private String getDate(File f) throws FileNotFoundException, IOException {
		Metadata md = new Metadata();
		String dateStr = null;
		tika.parse(new BufferedInputStream(new FileInputStream(f)), md);
		dateStr = md.get("dcterms:created");
		if ( dateStr != null ) {
			return dateStr;
		} else {
			System.out.println(md);
			return UNKNOWN_DATE;
		}
	}
}

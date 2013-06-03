package uk.bl.dpt.utils.duplicat.report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;

import uk.bl.dpt.utils.duplicat.exec.LuceneDA;

public class BeowulfCompare {

	private static final String IDXPATH = "C:/DPR/Beowulf/both/dbEnv";
	private static final String PHOTOSRC = "B:/Beowulf";
	private static final String WSRC = "W:/beowulf";

	public static void main(String[] args) {
		BeowulfCompare mi = new BeowulfCompare();
		try {
			mi.go();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void go() throws Exception {
		System.out.println("<h1>DPT Beowulf Comparison</h1>");
		System.out.println("<h2>Sources:</h2><p> \\\\digipres1\\w\\beowulf</p><p> \\\\p8l-nas2\\photographic studio data\\from drive\\Beowulf</p>");
		System.out.println("<hr />");
		
		IndexReader reader = LuceneDA.getIndexReader(LuceneDA.IDX.FILEINFO,
				IDXPATH);
		
		IndexSearcher searcher = LuceneDA.getIndexSearcher(LuceneDA.IDX.FILEINFO, IDXPATH);
		Term photoSrcTerm = new Term(LuceneDA.FIELD_SOURCE, PHOTOSRC);
		Term wSrcTerm = new Term(LuceneDA.FIELD_SOURCE, WSRC);
		int numDocsP = searcher.docFreq(photoSrcTerm);
		int numDocsW = searcher.docFreq(wSrcTerm);
		
		Map<String, List<Document>> docs = new HashMap<String, List<Document>>();

		int numDocs = reader.numDocs();
		
		String sha256 = null;
		List<Document> docList = null;
		List<String> paths = null;
		System.out.println("<html><title>DPT</title><body style=\"font-family: sans-serif;\">");

		System.out.println("<h2> Total Number of Files: " + numDocs + "</h2>");
		System.out.println("<h2> Total Number of Files on W: " + numDocsW + "</h2>");
		System.out.println("<h2> Total Number of Files on /from disk: " + numDocsP + "</h2>");
		
		System.out.println("<hr />");

		for (int i = 0; i < numDocs; i++) {
			Document d = reader.document(i);
			sha256 = d.get(LuceneDA.FIELD_SHA256);
			if ( docs.containsKey(sha256)) {
				docList = docs.get(sha256);

			} else {
				docList = new ArrayList<Document>();
			}
			docList.add(d);
			docs.put(sha256, docList);
		}
		
		reader.close();
		
		Map<String, List<String>> shas1 = new HashMap<String, List<String>>();
		
		for( String key: docs.keySet() ) {
			List<Document> storedDocs = docs.get(key);
			int copies = storedDocs.size();
			if ( copies == 1 ) {
				for ( Document d: storedDocs ) {
					if ( shas1.containsKey(d.get(LuceneDA.FIELD_SOURCE)) ) {
						paths = shas1.get(d.get(LuceneDA.FIELD_SOURCE));
					} else {
						paths = new ArrayList<String>();
					}
					paths.add(d.get(LuceneDA.FIELD_PATH));
					shas1.put(d.get(LuceneDA.FIELD_SOURCE), paths);
				}
			}
		}
		
		for ( String key: shas1.keySet() ) {
			Object[] pathList = shas1.get(key).toArray();
			Arrays.sort(pathList);
			System.out.println("<h2>Files Only On " + key + " [" +pathList.length + "]" + "</h2>");
			System.out.println("<table>");
			for ( String path: shas1.get(key) ) {
				System.out.println("<tr><td>" + path + "</td><tr>");
			}
			System.out.println("</table><hr />");
		}
		
		System.out.println("<h2>Duplicate files</h2>");
				
		for( String key: docs.keySet() ) {
			List<Document> storedDocs = docs.get(key);
			int copies = storedDocs.size();
			if ( copies > 2 ) {
				Document forSha = storedDocs.get(0);
				System.out.println("<table>");
				System.out.println("<tr><th style=\"background: #ccc; padding: 2px;\">" + forSha.get(LuceneDA.FIELD_SHA256) + "</th></tr>");
				
				for ( Document d: storedDocs ) {
					System.out.println("<tr><td>" + d.get(LuceneDA.FIELD_PATH) + "</td></tr>");
				}
				System.out.println("</table>");
			}
		}

		System.out.println("- EOM -");
		System.out.println("</body></html>");
	}
}
// IndexReader reader = LuceneDA.getIndexSearcher(LuceneDA.IDX.FILEINFO,
// IDXPATH);
//
// Term photoTerm = new Term(LuceneDA.FIELD_SOURCE, PHOTOSRC);
//
// int freq = scr.docFreq(photoTerm);
//
// TermQuery q = new TermQuery(photoTerm);
//
// TopDocs hits = scr.search(q, freq);
//
// System.out.println(" GOT " + hits.totalHits);
//
// ScoreDoc[] docs = hits.scoreDocs;
//
// for ( int i = 0; i < freq; i++ ) {
// Document d = scr.doc(docs[i].doc);
// Term t2 = new Term(LuceneDA.FIELD_SHA256, d.get(LuceneDA.FIELD_SHA256));
// int freq2 = scr.docFreq(t2);
// System.out.println(d.get(LuceneDA.FIELD_PATH) + " --> " + freq2);
// }
// }
// }

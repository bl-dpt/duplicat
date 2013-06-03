package uk.bl.dpt.utils.duplicat.report;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;

import uk.bl.dpt.utils.duplicat.exec.LuceneDA;

public class MissingFiles {
	
	private static final String X = "C:/OSMaps/X/dbEnv";
	private static final String W = "C:/OSMaps/W/dbEnv";
	private static final String D = "C:/OSMaps/D/dbEnv";
	private static final String PROGRESS = " %d/%d\n";
	private static final String NOT_FOUND = "In D but not W: %s\n";
	
	public static void main(String[] args) {
		MissingFiles mi = new MissingFiles();
		try {
			mi.go();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void go() throws IOException, ParseException {
		IndexSearcher wsearch = LuceneDA.getIndexSearcher(LuceneDA.IDX.FILEINFO, W);
		IndexReader copyReader = LuceneDA.getIndexReader(LuceneDA.IDX.FILEINFO, X);
	
		int numDocs = copyReader.numDocs();
		
		System.out.println("NumDocs: " + numDocs);
		
		for ( int i = 0; i < copyReader.numDocs(); i++ ) {
			if ( ( i % 100000 ) == 0 ) {
				System.out.printf(PROGRESS, i, numDocs);
			}
			Document doc = copyReader.document(i);
//			System.out.println(doc.get(LuceneDA.FIELD_SHA256));
			TermQuery q = new TermQuery(new Term(LuceneDA.FIELD_SHA256, doc.get(LuceneDA.FIELD_SHA256)));
			TopScoreDocCollector results = TopScoreDocCollector.create(10, true);
			wsearch.search(q, results);
			if ( results.getTotalHits() == 0 ) {
				System.out.printf(NOT_FOUND, doc.get(LuceneDA.FIELD_PATH));
			}
			
		}
		System.out.printf(PROGRESS, numDocs, numDocs);
		System.out.println("Done");
		
	}
}

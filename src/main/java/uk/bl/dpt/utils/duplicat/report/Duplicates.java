package uk.bl.dpt.utils.duplicat.report;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import uk.bl.dpt.utils.duplicat.exec.LuceneDA;

public class Duplicates {

	Map<String, List<String>> dupMap;

	public static void main(String[] args) {
		Duplicates duplicates = new Duplicates();
		try {
			duplicates.report();
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void report() throws CorruptIndexException, IOException {
		dupMap = new HashMap<String, List<String>>();

		IndexReader reader = null;
		reader = LuceneDA.getIndexReader(LuceneDA.IDX.FILEINFO, "C:/dbEnv/");

		int j = 0;

		for (int i = 0; i < reader.numDocs(); i++) {
			j++;
			if ((j % 1000) == 0) {
				System.out.println("Progress: " + j);
			}
			Document d = reader.document(i);
			String sha256 = d.get(LuceneDA.FIELD_SHA256);
			String path = d.get(LuceneDA.FIELD_PATH);
			int numCopies = reader.docFreq(new Term(LuceneDA.FIELD_SHA256,
					sha256));
			if (numCopies > 1) {
				List<String> paths;
				if (dupMap.containsKey(sha256)) {
					paths = dupMap.get(sha256);
				} else {
					paths = new ArrayList<String>();
				}
				paths.add(path);
				dupMap.put(sha256, paths);
			}
		}

		FileOutputStream fpo = null;
		File file;

		System.out.println("Writing report...");

		try {
			file = new File("./osmaps_duplicate_report_over4.html");
			fpo = new FileOutputStream(file);

			if (!file.exists()) {
				file.createNewFile();
			}

			write(fpo, "<html>\n");
			write(fpo, "<head>\n");
			write(fpo,
					"<title> DigiPres1 - W:\\OSMaps - Duplicates - 4 or more copies </title>\n");
			write(fpo, "</head>\n");
			write(fpo, "<style>\n");
			write(fpo,
					".dupset { border: 1px solid #ccc; padding: 4px; margin: 10px; }\n");
			write(fpo, ".dupsetSha { background: #bbb; }\n");
			write(fpo, ".dupsetCount { background: #ddd; }\n");
			write(fpo, "</style>\n");
			write(fpo, "<body>\n");
			write(fpo,
					"<h1>DigiPres1 - W:\\OSMaps - Duplicates - 4 or more copies </h1>\n");
			write(fpo, "<h2>Number of duplicated files: " + dupMap.size()
					+ " (not all are shown)</h2>\n");

			for (String shaKey : dupMap.keySet()) {
				List<String> paths = dupMap.get(shaKey);
				if (paths.size() >= 4) {
					write(fpo, "<div class=\"dupset\">\n");
					write(fpo, "<div class=\"dupsetSha\">" + shaKey
							+ "</div>\n");
					write(fpo, "<div class=\"dupsetCount\">Duplicate Count: "
							+ paths.size() + "</div>\n");
					write(fpo, "<div class=\"dupsetPaths\">\n");
					for (String path : paths) {
						write(fpo, "<div class=\"dupsetPath\">" + path
								+ "</div>\n");
					}
					write(fpo, "</div>");
					write(fpo, "</div><!--/dupset-->\n");
				}
			}
			write(fpo, "</body>\n");
			write(fpo, "</html>\n");

			fpo.flush();
			fpo.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Report done.");
	}

	private void write(FileOutputStream o, String s) throws IOException {
		byte[] b = s.getBytes();
		o.write(b);
	}
}

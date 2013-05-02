package uk.bl.dpt.utils.duplicat.reports;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.bl.dpt.utils.duplicat.entities.Config;
import uk.bl.dpt.utils.duplicat.entities.FileInfoEntity;
import uk.bl.dpt.utils.duplicat.entities.Path;
import uk.bl.dpt.utils.duplicat.fileinfo.FileInfoDA;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;

public class SourceDuplicationSummaryReport {
	private Config conf;

	@SuppressWarnings("unused")
	private SourceDuplicationSummaryReport() {

	}

	public SourceDuplicationSummaryReport(Config conf) {
		this.conf = conf;
	}

	public String getReport() throws DatabaseException {
		long numUniqueFiles = 0;
		long numUniqueFilesWithCopiesFromEachSource = 0;
		StringBuilder sb = new StringBuilder();
		sb.append(" Number of sources: " + conf.getPaths().length + "\n");

		FileInfoDA fida = new FileInfoDA(conf);
		EntityCursor<FileInfoEntity> fiec = fida.getEntities();

		for (FileInfoEntity fie : fiec) {
			numUniqueFiles++;

			List<Path> fiePaths = fie.getPaths();
			long entityPathLength = fiePaths.size();

			Map<String, String> inputPaths = new HashMap<String, String>();
			for ( String ip : conf.getPaths() ) {
				inputPaths.put(ip, ip);
			}
			
			long inputPathLength = inputPaths.size();

			for (Path p : fiePaths) {
				if (inputPaths.containsKey(p.source)) {
					inputPaths.remove(p.source);
				}
			}

			if (inputPaths.size() == 0 && (entityPathLength == inputPathLength)) {
				numUniqueFilesWithCopiesFromEachSource++;
			} else {
				System.out.println(fie);
			}
		}
		
		sb.append("Number of unique files: " + numUniqueFiles + "\n");
		sb.append("Number of unique files in all paths: " + numUniqueFilesWithCopiesFromEachSource + "\n");

		return sb.toString();
	}
}

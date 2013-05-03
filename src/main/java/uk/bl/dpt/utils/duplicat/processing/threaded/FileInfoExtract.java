package uk.bl.dpt.utils.duplicat.processing.threaded;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import uk.bl.dpt.utils.duplicat.entities.Config;
import uk.bl.dpt.utils.duplicat.entities.FilePathEntity;
import uk.bl.dpt.utils.duplicat.fileinfo.FileInfoDA;
import uk.bl.dpt.utils.duplicat.filelist.FileListDA;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;

public class FileInfoExtract {

	private static final String WAITING = "Waiting for threads to complete...";
	Config conf;

	@SuppressWarnings("unused")
	private FileInfoExtract() {

	}

	public FileInfoExtract(Config conf) throws DatabaseException {
		this.conf = conf;
	}

	public void go() throws DatabaseException {
		double processed = (double) 1;
		FileInfoDA fida = new FileInfoDA(conf);
		FileListDA flda = new FileListDA(conf);
		double totalFiles = (double) flda.count();
		
		ExecutorService pool = Executors.newFixedThreadPool(4);
		EntityCursor<FilePathEntity> fileListCursor = flda.getEntities();
		FilePathEntity fpe = null;
		
		System.out.printf("Posted %.0f/%.0f [%2.2f %%]\n", 0.0, totalFiles, 0.0);

		//TODO - rather than all this, just use a blocking queue? Will need to us
		//ThreadPoolExecutor direct rather than ExecutorService.
		
		while ((fpe = fileListCursor.next()) != null) {
			if ( ( processed % 1000 ) == 0 ) {
				System.out.printf("Posted %.0f/%.0f [%2.2f %%] - sleeping for %d secs\n", processed, totalFiles, ((processed/totalFiles)*100.0), (conf.getThreadWait()/1000));
				try {
					Thread.sleep(conf.getThreadWait());
				} catch (InterruptedException e) {
					// nothing really...
				}
			}
			if ( processed >= totalFiles ) { 
				System.out.printf("Posted %.0f/%.0f [%2.2f %%]\n", processed, totalFiles, ((processed/totalFiles)*100.0));
			}
			Thread fred = new FileInfoExtractThread(fpe, fida);
			pool.execute(fred);
			processed++;
		}
		pool.shutdown();
		if (pool.isShutdown()) {
			System.out.println(WAITING);
			while (!pool.isTerminated()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// don't care! :-)
				}
			}
		}
		System.out.println("Pool done");
		fileListCursor.close();
		fida.close();
		flda.close();
	}
}

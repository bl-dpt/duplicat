package uk.bl.dpt.utils.duplicat.exec;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

public class FileInfoExtract {

	private static final String POSTED = "Posted %.0f/%.0f [%2.2f%%] [%d]\n";
	private static final String STATUS = "Completed: %d, Count: %d Active: %d \n";
		
	Config conf;

	@SuppressWarnings("unused")
	private FileInfoExtract() {

	}

	public FileInfoExtract(Config conf) {
		this.conf = conf;
	}

	public void go() {
		double processed = 1.0d;
		IndexReader flir = null;
		IndexWriter fiiw = null;
		
		try {
			flir = LuceneDA.getIndexReader(LuceneDA.IDX.FILELIST, conf.getDBEnv());
			fiiw = LuceneDA.getIndexWriter(LuceneDA.IDX.FILEINFO, conf.getDBEnv());;
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		double numDocs = (double) flir.numDocs();
		
		ExecutorService espool = Executors.newFixedThreadPool(conf.getNumThreads());
		ThreadPoolExecutor pool = null;
		
		if ( espool instanceof ThreadPoolExecutor ) { // is in Java 1.7 at least... :)
			pool = (ThreadPoolExecutor) espool;
		}
		
		if ( pool == null ) {
			System.out.println("Unable to get ThreadPoolExecutor");
			// Could just carry on using espool instead...
			System.exit(-1);
		}
		
		System.out.printf(POSTED, 0.0, numDocs, 0.0, pool.getTaskCount());
		System.out.printf(STATUS, pool.getCompletedTaskCount(), pool.getTaskCount(), pool.getActiveCount());


		// TODO - rather than all this, just use a blocking queue? Will need to
		// us
		// ThreadPoolExecutor direct rather than ExecutorService.

		for (int i = 0; i < numDocs; i++) {
			Document doc = null;
			if (!(flir.isDeleted(i))) {
				try {
					doc = flir.document(i);
				} catch (CorruptIndexException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				if ((processed % conf.getPostCount()) == 0) {
					System.out
							.printf(POSTED,
									processed, numDocs,
									((processed / numDocs) * 100.0),
									(conf.getThreadWait() / 1000), pool.getTaskCount());
					System.out.printf(STATUS, pool.getCompletedTaskCount(), pool.getTaskCount(), pool.getActiveCount());
					try {
						fiiw.commit();
						Thread.sleep(conf.getThreadWait());
					} catch (InterruptedException e) {
						// nothing really...
					} catch (CorruptIndexException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				if (processed >= numDocs) {
					System.out
							.printf(POSTED, processed,
									numDocs, ((processed / numDocs) * 100.0), pool.getTaskCount());
					System.out.printf(STATUS, pool.getCompletedTaskCount(), pool.getTaskCount(), pool.getActiveCount());
				}
				Thread fred = new FileInfoExtractThread(doc, fiiw);
				pool.execute(fred);
				processed++;
			}

		}

		pool.shutdown();

		if (pool.isShutdown()) {
			System.out.printf(STATUS, pool.getCompletedTaskCount(), pool.getTaskCount(), pool.getActiveCount());
			while (!pool.isTerminated()) {
				try {
					Thread.sleep(1000);
					System.out.printf(STATUS, pool.getCompletedTaskCount(), pool.getTaskCount(), pool.getActiveCount());
				} catch (InterruptedException e) {
					// don't care! :-)
				}
			}
		}
		System.out.println("Pool done");
		try {
			fiiw.commit();
			fiiw.close();
			flir.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}

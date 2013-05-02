package uk.bl.dpt.utils.duplicat.main;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import uk.bl.dpt.utils.duplicat.entities.Config;
import uk.bl.dpt.utils.duplicat.entities.FileInfoEntity;
import uk.bl.dpt.utils.duplicat.entities.FilePathEntity;
import uk.bl.dpt.utils.duplicat.entities.Path;
import uk.bl.dpt.utils.duplicat.fileinfo.FileInfoDA;
import uk.bl.dpt.utils.duplicat.filelist.FileListDA;
import uk.bl.dpt.utils.duplicat.processing.threaded.FileInfoExtract;
import uk.bl.dpt.utils.duplicat.reports.SourceDuplicationSummaryReport;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;

public class Main {
	private static long startTime;
	private static long endTime;

	public static void main(String[] args) {
		startTime = System.currentTimeMillis();
		Options opts = new Options();
		Option cOpt = new Option("c", true,
				"The location of the configuration properties file");
		cOpt.setRequired(true);
		opts.addOption(cOpt);
		opts.addOption("l", false,
				"Lists the content of the dbenv/store given in conf file");
		opts.addOption("f", false, "Create file path list");
		opts.addOption("i", false, "Create file info list");
		opts.addOption("p", false, "Purge idatabases before starting");
		opts.addOption("m", false, "Purge file list before starting");
		opts.addOption("n", false, "Purge file info before starting");
		opts.addOption("r", false, "Run a report, giving name as parameter");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(opts, args);
		} catch (MissingOptionException e) {
			System.out.println("You must specify a config file using -c FILE");
			System.exit(-1);
		} catch (ParseException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		Main mi = new Main();
		Config conf = null;

		System.out.println("Duplicat");
		System.out.println("--------");
		System.out.println(" Begin");

		try {
			conf = new Config(cmd.getOptionValue('c'));
		} catch (IOException e) {
			System.out.println("You must specify a config file using -c FILE");
			System.exit(-1);
		}

		if (cmd.hasOption('l')) {
			try {
				mi.listDatabase(conf);
			} catch (DatabaseException e) {
				System.out
						.println("Unable to list database: " + e.getMessage());
			}
		} else if (cmd.hasOption('r')) {
			try {
				SourceDuplicationSummaryReport r = new SourceDuplicationSummaryReport(conf);
				System.out.println(r.getReport());
			} catch (DatabaseException e) {
				System.out.println("Unable to report: " + e.getMessage());

			}
		} else {
			if (cmd.hasOption('p')) {
				mi.purgeDatabases(conf);
			}
			if (cmd.hasOption('m')) {
				mi.purgeFileList(conf);
			}
			if (cmd.hasOption('n')) {
				mi.purgeFileInfo(conf);
			}
			if (cmd.hasOption('f')) {
				mi.readFiles(conf);
			}
			if (cmd.hasOption('i')) {
				try {
					mi.createFileInfo(conf);
				} catch (DatabaseException e) {
					System.out.println("Unable to store file info: "
							+ e.getMessage());
				}
			}
		}

		endTime = System.currentTimeMillis();
		float execTime = (endTime - startTime) / 1000;

		System.out.println(" End");
		System.out.println("--------");

		if (execTime < 60) {
			System.out.printf("Execution time %8.2f secs\n", execTime);
		} else if (execTime / 60 < 60) {
			System.out.printf("Execution time %8.2f mins\n", execTime / 60);
		} else {
			System.out.printf("Execution time %8.2f hours\n",
					execTime / 60 / 60);
		}
	}

	private void purgeFileList(Config conf) {
		System.out.print("  Purging DB file list");
		FileListDA fl = null;
		try {
			fl = new FileListDA(conf);
			fl.purge();
		} catch (DatabaseException e) {
			doErrorAndExit("Purge file list failed: database error: "
					+ e.getMessage());
		}
		fl.close();
	}

	private void purgeFileInfo(Config conf) {
		System.out.print("  Purging DB file info");
		FileInfoDA fi = null;
		try {
			fi = new FileInfoDA(conf);
			fi.purge();
		} catch (DatabaseException e) {
			doErrorAndExit("Purge file info failed: database error: "
					+ e.getMessage());
		}
		fi.close();
		System.out.println(" - done");
	}

	private void purgeDatabases(Config conf) {
		System.out.print("  Purging DB environment");
		FileListDA fl = null;
		try {
			fl = new FileListDA(conf);
			fl.purge();
		} catch (DatabaseException e) {
			doErrorAndExit("Purge file list failed: database error: "
					+ e.getMessage());
		}
		fl.close();
		FileInfoDA fi = null;
		try {
			fi = new FileInfoDA(conf);
			fi.purge();
		} catch (DatabaseException e) {
			doErrorAndExit("Purge file info failed: database error: "
					+ e.getMessage());
		}
		fi.close();
		System.out.println(" - done");
	}

	private void readFiles(Config conf) {
		System.out.print("  Getting file list");
		FileListDA flist = null;
		try {
			flist = new FileListDA(conf);
		} catch (DatabaseException e) {
			doErrorAndExit("Database error: " + e.getMessage());
		}

		for (String path : conf.getPaths()) {
			File root = new File(path);
			if (root.exists()) {
				storeFilePathsRecursive(flist, root, path);
			} else {
				System.out.println(" Skipping: " + path + ": DOES NOT EXIST");
			}
		}
		long recCount = -1;
		try {
			recCount = flist.count();
		} catch (DatabaseException e) {
			// ignore
		}
		System.out.println(" - found " + recCount);
		flist.close();
	}

	private void storeFilePathsRecursive(FileListDA fl, File f, String source) {
		if (f.isDirectory()) {
			File[] files = f.listFiles();
			if (files != null) {
				for (File nextfile : files) {
					storeFilePathsRecursive(fl, nextfile, source);
				}
			} else {
				// log that we found a directory with no files?
			}
		} else { // Assume we found a file!
			Path path = new Path();
			path.path = f.getAbsolutePath();
			path.source = source;
			FilePathEntity ent = new FilePathEntity(path);
			try {
				fl.put(ent);
			} catch (DatabaseException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}

	private void listDatabase(Config conf) throws DatabaseException {
		FileListDA fileList = new FileListDA(conf);

		EntityCursor<FilePathEntity> fpec = fileList.getEntities();
		System.out.println("  FilePathEntities Listing ");
		System.out.println("  ======================== ");
		FilePathEntity ent;
		while ((ent = fpec.next()) != null) {
			System.out.println(ent);
		}
		fpec.close();

		FileInfoDA fileInfo = new FileInfoDA(conf);
		EntityCursor<FileInfoEntity> fiec = fileInfo.getEntities();
		System.out.println("  FileInfoEntities Listing ");
		System.out.println("  ======================== ");
		FileInfoEntity enti;
		while ((enti = fiec.next()) != null) {
			System.out.println(enti);
		}
		fiec.close();

		fileList.close();
		fileInfo.close();
	}

	private void createFileInfo(Config conf) throws DatabaseException {
		FileInfoExtract extractor = new FileInfoExtract(conf);
		System.out.println("  Get info for each file");
		extractor.go();
		System.out.println("  Get info done");
	}

	private void doErrorAndExit(String msg) {
		System.out.println("\nError:\n" + msg + "\n\n");
		System.exit(-1);
	}
}

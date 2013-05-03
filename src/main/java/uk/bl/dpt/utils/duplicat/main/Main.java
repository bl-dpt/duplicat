package uk.bl.dpt.utils.duplicat.main;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import uk.bl.dpt.utils.duplicat.entities.Config;
import uk.bl.dpt.utils.duplicat.execsys.DuplicatException;
import uk.bl.dpt.utils.duplicat.execsys.DuplicatExecutor;
import uk.bl.dpt.utils.duplicat.execsys.bdb.BDBExecImpl;
import uk.bl.dpt.utils.duplicat.execsys.lucene.LuceneExecImpl;

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
		
		DuplicatExecutor mi = null;
		try {
			mi = new LuceneExecImpl(conf);
		} catch (DuplicatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}

		if (cmd.hasOption('l')) {
			mi.listDatabase();
		} else if (cmd.hasOption('r')) {
		} else {
			if (cmd.hasOption('p')) {
				mi.purgeDatabases();
			}
			if (cmd.hasOption('m')) {
				mi.purgeFileList();
			}
			if (cmd.hasOption('n')) {
				mi.purgeFileInfo();
			}
			if (cmd.hasOption('f')) {
				mi.createFileList();
			}
			if (cmd.hasOption('i')) {
				mi.createFileInfo();
			}
		}
		
		mi.close();

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
}

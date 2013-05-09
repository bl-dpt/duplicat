package uk.bl.dpt.utils.duplicat.main;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import uk.bl.dpt.utils.duplicat.exec.Config;
import uk.bl.dpt.utils.duplicat.exec.LuceneExec;

public class Main {
	private static long startTime;
	private static long endTime;
	private static Options opts;

	public static void main(String[] args) {
		startTime = System.currentTimeMillis();
		opts = new Options();
		opts.addOption("c", true,
				"The location of the configuration properties file");
		opts.addOption("l", false,
				"Lists the content of the indexes given in conf file");
		opts.addOption("f", false, "Create file path list");
		opts.addOption("i", false, "Create file info list");
		opts.addOption("p", false, "Purge indexes before starting");
		opts.addOption("m", false, "Purge file list before starting");
		opts.addOption("n", false, "Purge file info before starting");
		opts.addOption("h", false, "Display help message");
//		opts.addOption("r", false, "Run a report, giving name as parameter");

		System.out.println("Duplicat");
		System.out.println("--------");
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(opts, args);
		} catch (ParseException pe) {
			System.out.println("Unable to parse command line: " + pe.getMessage());
			doUsageAndExit();
		}
		
		if ( cmd.hasOption('h') ) {
			doUsageAndExit();
		}
		
		if (!cmd.hasOption('c') ) {
			System.out.println(" You MUST specify a config file for all other actions");
			doUsageAndExit();
		}
		
		Config conf = null;
		try {
			conf = new Config(cmd.getOptionValue('c'));
		} catch (IOException e) {
			System.out.println(" You MUST specify a valid config file for all other actions");
			doUsageAndExit();
		}
		
		LuceneExec mi =  new LuceneExec(conf);
		
		if ( cmd.hasOption('l')) {
			mi.listDatabase();
		} else {
			// The order of these is important as we step through each sequentially.
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

		System.out.println("--------");

		System.out.println("ExecTime: " + execTime);
		
		if (execTime < 60) {
			System.out.printf("Execution time %8.2f secs\n", execTime);
		} else if (execTime / 60 < 60) {
			System.out.printf("Execution time %8.2f mins\n", execTime / 60);
		} else {
			System.out.printf("Execution time %8.2f hours\n",
					execTime / 60 / 60);
		}
	}
	
	private static void doUsageAndExit() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "java -jar duplicat.jar", opts );
		System.exit(0);
	}
}

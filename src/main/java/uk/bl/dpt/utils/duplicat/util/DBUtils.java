package uk.bl.dpt.utils.duplicat.util;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.PrimaryIndex;

public class DBUtils {
	public static void purgeIdx(PrimaryIndex<?, ?> idx) {
		try {
			EntityCursor<?> ec = idx.entities();
			while ( ec.next() != null ) {
				ec.delete();
			}
			ec.close();
		} catch (DatabaseException e) {
			e.printStackTrace();
			System.exit(-1);
		} 
	}

	public static void countIdx(PrimaryIndex<?, ?> idx) {
		try {
			System.out.println(" Index: " + idx.count());
		} catch (DatabaseException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public static long getCountIdx(PrimaryIndex<?, ?> idx) {
		try {
			return idx.count();
		} catch (DatabaseException e) {
			return -1;
		}
	}

	public static void listIdx(PrimaryIndex<?, ?> idx) {
		try {
			EntityCursor<?> ec = idx.entities();
			Object o;
			while ( (o = ec.next()) != null ) {
				System.out.println(o);
			}
			ec.close();
		} catch (DatabaseException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}

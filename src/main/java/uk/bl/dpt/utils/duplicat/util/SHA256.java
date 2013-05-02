package uk.bl.dpt.utils.duplicat.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA256 {
	public String digest(File f) throws IOException {
		return digest(f, true);
	}
	
	public String digest(File f, boolean uppercase) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(f));
		return digest(is, uppercase);
		
	}

	public String digest(InputStream fis, boolean uppercase)
			throws IOException {
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			System.err.println("ERROR: NoSuchAlgorithm: Which is odd. Exiting.");
			System.exit(0);
		}

		byte[] data = new byte[8192];
		int read = 0;
		while ((read = fis.read(data)) != -1) {
			md.update(data, 0, read);
		}
		fis.close();
		data = null;

		byte[] mdbytes = md.digest();

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < mdbytes.length; i++) {
			sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16)
					.substring(1));
		}
		
		mdbytes = null;
		
		if (uppercase) {
			return sb.toString().toUpperCase();
		} else {
			return sb.toString();
		}
	}
}

package de.martin70m.common.io;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipReader {

	public static int upzip(String filename, String outpath) {
		// create a buffer to improve copy performance later.
		byte[] buffer = new byte[2048];
		int filecounter = 0;

		Path outDir = Paths.get(outpath);
		try (

				// we open the zip file using a java 7 try with resources block so
				// that we don't need a finally.
				ZipInputStream stream = new ZipInputStream(new FileInputStream(filename));) {

			Files.walkFileTree(outDir, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

			});

			// now iterate through each file in the zip archive. The get
			// next entry call will return a ZipEntry for each file in
			// the stream
			ZipEntry entry;
			while ((entry = stream.getNextEntry()) != null) {
				filecounter++;
				// We can read the file information from the ZipEntry.
				/*
				 * String fileInfo = String.format("Entry: [%s] len %d added %TD",
				 * entry.getName(), entry.getSize(), new Date(entry.getTime()));
				 */

				Path filePath = outDir.resolve(entry.getName());

				// Now we can read the file data from the stream. We now
				// treat the stream like a usual input stream reading from
				// it until it returns 0 or less.
				try (FileOutputStream output = new FileOutputStream(filePath.toFile())) {
					int len;
					while ((len = stream.read(buffer)) > 0) {
						output.write(buffer, 0, len);
					}
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return filecounter;
	}
}

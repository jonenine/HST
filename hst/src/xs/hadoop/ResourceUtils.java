package xs.hadoop;

import java.io.*;
import java.net.URL;

/**
 * 本地文件资源类
 */
public class ResourceUtils {

	public ResourceUtils() {
	}

	public static URL getPackageFragmentDir(Class clazzInPackage) {
		return clazzInPackage.getResource("");
	}

	public static URL getPackageRootDir(Class clazzInPackage) {
		return clazzInPackage.getResource("/");
	}

	public static InputStream getResourceInBundle(Class clazzInBundle,
			String pathInBundle) {
		return clazzInBundle.getResourceAsStream(pathInBundle);
	}

	public static byte[] getFileContentInLocalFileSystem(String filePath)
			throws Exception {
		BufferedInputStream input = new BufferedInputStream(
				new FileInputStream(filePath), 8192);
		return getFileContent(input);
	}

	public static byte[] getFileContent(InputStream input) throws Exception {
		return ((ByteArrayOutputStream) connectPipe(input,
				new ByteArrayOutputStream())).toByteArray();
	}

	public static String getFileContentInLocalFileSystem(String filePath,
			String charSet) throws Exception {
		return getFileContent(new FileInputStream(filePath), charSet);
	}

	public static String getFileContent(InputStream in, String charSet)
			throws Exception {
		StringBuffer buffer = new StringBuffer();
		InputStreamReader reader = new InputStreamReader(in, charSet);
		BufferedReader bufferedReader = new BufferedReader(reader, 8192);
		for (String line = null; (line = bufferedReader.readLine()) != null;)
			buffer.append((new StringBuilder(String.valueOf(line)))
					.append("\n").toString());

		bufferedReader.close();
		reader.close();
		return buffer.toString();
	}

	private static OutputStream connectPipe(InputStream input,
			OutputStream output) throws Exception {
		byte data[] = new byte[8192];
		int count;
		while ((count = input.read(data)) != -1) {
			output.write(data, 0, count);
			output.flush();
		}
		input.close();
		return output;
	}


}

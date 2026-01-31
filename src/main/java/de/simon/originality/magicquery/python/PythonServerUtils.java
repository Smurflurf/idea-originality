package de.simon.originality.magicquery.python;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.stream.Stream;

/**
 * Contains methods for finding the right python executable, even in a pyenv, to start the python server from Java.
 */
public class PythonServerUtils {
	/**
	 * Searches for a valid Python executable.
	 * First looks for pyenv using {@link #findPythonPyenv()}, then checks for 'python3' and 'python'.
	 * @return String Path to valid Python executable, null if none exists.
	 */
	public static String findPythonExecutable() {
		ArrayList<String> pythons = new ArrayList<String>();
		Path[] pyenv = PythonServerUtils.findPythonPyenv();
		if(pyenv != null)
			for(Path path : pyenv)
				pythons.add(path.toString());
		pythons.add("python3");
		pythons.add("python");
		
		int version = pythons.size();
		String firstFoundVersion = null;
		for(String python : pythons) {
			ProcessBuilder pb = new ProcessBuilder(python, "--version");

			pb.redirectErrorStream(true);
			try {
				Process p = pb.start();
				String output = new String(p.getInputStream().readAllBytes());
				System.out.println(output.strip() + " found.");
				firstFoundVersion = firstFoundVersion == null ? python : firstFoundVersion;
			} catch (Exception e) {
				version--;
			}
		}
		if(version==0) {
			System.err.println("No valid Python executable found. See magicquery.vectorizer.PythonServerUtils.");
			return null;
		} else {
			System.out.println(version + " valid Python executables found. Escalating " + firstFoundVersion + ".");
		}
		
		return firstFoundVersion;
	}
	
	/**
	 * If Python is installed in a pyenv, the Path is found and returned.
	 * @implNote only tested on linux
	 * @return Path to Python pyenv or null if none exists.
	 */
	public static Path[] findPythonPyenv() {
		ArrayList<Path> result = new ArrayList<Path>();
		
		Path pyenvRoot = Path.of(System.getProperty("user.home"), ".pyenv", "versions");
		if (Files.exists(pyenvRoot)) {
			try (var paths = Files.list(pyenvRoot)) {
				Stream<Path> pythonPath = paths
						.map(v -> v.resolve("bin").resolve("python"))
						.filter(Files::exists);
				
				pythonPath.forEach(pyenv -> result.add(pyenv));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result.toArray(Path[]::new);
	}
}

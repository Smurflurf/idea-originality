package de.simon.originality.magicquery;

public enum MagicNumbers {
	/**
	 * Time to wait for Python server crash after using the wrong python start command ("python" or "python3").
	 * Low number is OK.
	 */
	WAIT_FOR_PYTHON_PROCESS_CRASH_MS("2000"),
	/**
	 * Time to wait for the Python servers response ping after it got created.
	 * On slow hardware interpreting the Python code can take quite long, so a high number is OK.
	 */
	WAIT_FOR_PYTHON_PROCESS_START_MS("60000"),
	/**
	 * Time to wait for an answer from the Python servers vectorize endpoint.
	 * On slow hardware the vectorizing can take quite long, so a high number is OK.
	 */
	WAIT_FOR_VECTORIZING_SERVER_ANSWER_MS("60000"),
	/**
	 * File name of the vectorizer script, without .py
	 */
	VECTORIZER_SERVER_FILE_NAME("vectorizer_server");
	
	String value;
	private MagicNumbers(String value) {
		this.value= value;
	}
	public int asInteger() {
		return Integer.parseInt(value);
	}
	public String asString() {
		return value;
	}
}



public enum QueryResult {
	PASSED("PASSED"),
	FAILED("FAILED");
	
	private String title;
	
	QueryResult(String title) {
		this.title = title;
	}
	
	public String getTitle() {
		return this.title;
	}
}

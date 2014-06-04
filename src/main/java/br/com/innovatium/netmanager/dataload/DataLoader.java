package br.com.innovatium.netmanager.dataload;

public final class DataLoader {

	public static void main(String[] args) throws FileReadingException {
		new DataLoader().load();
	}

	public void load() throws FileReadingException {
		TableFilesReader reader = new TableFilesReader();
		try {
			reader.read();
		} catch (FileReadingException e) {
			System.err.println("Fail to begin data import. Message: "
					+ e.getMessage());
			throw e;
		}
	}
}

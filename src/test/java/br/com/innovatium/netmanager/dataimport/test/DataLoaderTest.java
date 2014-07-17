package br.com.innovatium.netmanager.dataimport.test;

import static org.junit.Assert.assertFalse;

import org.junit.Test;

import br.com.innovatium.netmanager.dataload.DataLoader;
import br.com.innovatium.netmanager.dataload.FileReadingException;

public class DataLoaderTest {

	@Test
	public void testDataImport() {
		DataLoader dataLoader = new DataLoader();
		try {
			dataLoader.load();
		} catch (FileReadingException e) {
			assertFalse("Fail on reading table files", true);
		}
	}
}

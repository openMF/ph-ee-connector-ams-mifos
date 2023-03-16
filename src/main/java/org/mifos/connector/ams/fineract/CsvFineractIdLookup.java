package org.mifos.connector.ams.fineract;

import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

@Component
public class CsvFineractIdLookup {
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private Map<String, Integer> lookupMap = new HashMap<>();
	
	public CsvFineractIdLookup() {
		try {
			readReader(Paths.get(ClassLoader.getSystemResource("PaymentTypes.csv").toURI()));
		} catch (URISyntaxException e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void readReader(Path path) {
		try (Reader reader = Files.newBufferedReader(path)) {
			readcsv(reader);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void readcsv(Reader reader) throws IOException {
		try (CSVReader csvReader = new CSVReader(reader)) {
			List<String[]> csvLines = csvReader.readAll();
			csvLines.remove(0);
			csvLines.forEach(elem -> lookupMap.put(elem[0], Integer.parseInt(elem[2])));
		} catch (CsvException e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public Integer getFineractId(String paymentTypeName) {
		return lookupMap.get(paymentTypeName);
	}
}

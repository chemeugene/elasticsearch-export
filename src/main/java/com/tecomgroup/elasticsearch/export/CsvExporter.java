package com.tecomgroup.elasticsearch.export;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CsvExporter {

	private final Logger LOGGER = LoggerFactory.getLogger(CsvExporter.class);

	@Autowired
	private JestClient jestApi;

	@Value("${bulk.size}")
	private Integer bulkSize;

	private final static String SEARCH_ALL_REQUEST = "{ \"query\": { \"match_all\": {} }, \"sort\": [{\"date\": {\"order\": \"desc\"}}] }";

	public void executeExport(String index, String[] columns,
			String outputFileName) {
		Search search = new Search.Builder(SEARCH_ALL_REQUEST)
				.addIndex(index)
				.setParameter(Parameters.SIZE, bulkSize)
				.setParameter(Parameters.SCROLL, "5m")
				.build();
		String scrollId = null;
		try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(Paths
				.get(outputFileName)))) {
			SearchResult result = jestApi.execute(search);
			Long docCount = result.getJsonObject().get("hits").getAsJsonObject()
					.get("total").getAsLong();
			LOGGER.info("Searched {} documents to export", docCount);
			scrollId = result.getJsonObject().get("_scroll_id").getAsString();
			//Write first batch or records
			List<String> records = result.getSourceAsStringList();
			writeAndFlushRecords(records, writer);
			//Write all remaining records			
			executeScroll(writer, scrollId);
		} catch (Exception e) {
			LOGGER.error("Unable to perform export", e);
		}
	}
	
	private void executeScroll(PrintWriter writer, String scrollId) {
		try {
			SearchScroll scroll = new SearchScroll.Builder(scrollId, "5m")
			.build();			
			JestResult result = jestApi.execute(scroll);
			List<String> records = result.getSourceAsStringList();
			writeAndFlushRecords(records, writer);
			System.gc();
			if (records.size() > 0) {
				executeScroll(writer, scrollId);
			}
		} catch (IOException e) {
			LOGGER.error("Exception while fetch records from elasticsearch", e);
		}
	}
	
	private void writeAndFlushRecords(List<String> records, PrintWriter writer) throws IOException {
		for (String record : records) {
			writer.write(record + System.lineSeparator());
		}
		writer.flush();
	}

}

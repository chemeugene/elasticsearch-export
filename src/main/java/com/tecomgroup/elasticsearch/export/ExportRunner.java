package com.tecomgroup.elasticsearch.export;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class ExportRunner implements CommandLineRunner {
	
	@Autowired
	private CsvExporter csv;

	public void run(String... args) throws Exception {
	
		Options options = new Options();
		
		Option indexName = new Option("i", "index", true, "Index name to export");
		indexName.setRequired(false);
		options.addOption(indexName);
		
		Option columns = new Option("c", "columns", true, "List of comma separated columns to export");
		columns.setRequired(false);
		columns.setArgs(Option.UNLIMITED_VALUES);
		columns.setValueSeparator(',');
		options.addOption(columns);
		
		Option outputFormat = new Option("f", "format", false, "Output format: csv by default");
		columns.setRequired(false);
		options.addOption(outputFormat);
		
		Option outputFileName = new Option("d", "destination", false, "Output file name");
		outputFileName.setRequired(false);
		options.addOption(outputFileName);
		
		CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("export", options);
            System.exit(1);
            return;
        }
        
        String indexNameValue = cmd.getOptionValue("index");
        String[] columnsValue = cmd.getOptionValues("columns");
        String outputFormatValue = cmd.getOptionValue("format");
        outputFormatValue = outputFormatValue == null ? "csv" : outputFormatValue;
        String outputFileNameValue = cmd.getOptionValue("destination");
        outputFileNameValue = outputFileNameValue == null ? "export-result."+outputFormatValue : outputFileNameValue;
        
        csv.executeExport(indexNameValue, columnsValue, outputFileNameValue);
	}

}

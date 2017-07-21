package com.tecomgroup.elasticsearch.export;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {
	
	@Value("${elasticsearch.url}")
	private String elasticSearchUrl;
	
	@Value("${elasticsearch.readTimeoutMs}")
	private Integer readTimeoutMs;

	@Bean
	public JestClient jestClient() {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder(
				elasticSearchUrl).multiThreaded(true).readTimeout(readTimeoutMs).build());
		
		return factory.getObject();
	}
}

package com.neu.flexquery.config;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;

@Configuration
public class ElasticSearchConfig extends AbstractElasticsearchConfiguration {

    /**
     * When security is enabled with AbstractElasticsearchConfiguration extended
     */
    @Bean
    @Override
    public RestHighLevelClient elasticsearchClient() {

        final ClientConfiguration clientConfiguration =
                ClientConfiguration
                        .builder()
                        .connectedTo("localhost:9200")
                        .withBasicAuth("elastic","9k1UT=TAuQzGjpvZQDvh")
                        .build();

        return RestClients.create(clientConfiguration).rest();
    }
}
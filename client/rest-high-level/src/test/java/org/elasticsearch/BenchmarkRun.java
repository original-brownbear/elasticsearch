package org.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

public class BenchmarkRun extends ESTestCase {

    public static void main(String[] args) throws IOException {
        try (RestHighLevelClient highLevelClient
                     = new RestHighLevelClientBuilder(RestClient.builder(new HttpHost("127.0.0.1", 9200)).build()).build()) {
            final BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < 10000; i++) {
                bulkRequest.add(new IndexRequest("index-" + (i % 100)).source(Map.of("foo", "bar")));
            }
            highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        }
    }
}

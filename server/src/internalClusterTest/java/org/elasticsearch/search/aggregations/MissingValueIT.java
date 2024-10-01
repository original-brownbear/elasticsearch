/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoBounds;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoCentroid;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.closeTo;

@ESIntegTestCase.SuiteScopeTestCase
public class MissingValueIT extends ESIntegTestCase {

    @Override
    protected int maximumNumberOfShards() {
        return 2;
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").setMapping("date", "type=date", "location", "type=geo_point", "str", "type=keyword").get());
        indexRandom(
            true,
            prepareIndex("idx").setId("1").setSource(),
            prepareIndex("idx").setId("2").setSource("str", "foo", "long", 3L, "double", 5.5, "date", "2015-05-07", "location", "1,2")
        );
    }

    public void testUnmappedTerms() {
        assertNoFailuresAndResponse(response -> {
            Terms terms = response.getAggregations().get("my_terms");
            assertEquals(1, terms.getBuckets().size());
            assertEquals(2, terms.getBucketByKey("bar").getDocCount());
        }, prepareSearch("idx").addAggregation(terms("my_terms").field("non_existing_field").missing("bar")));
    }

    public void testStringTerms() {
        for (ExecutionMode mode : ExecutionMode.values()) {
            assertNoFailuresAndResponse(response -> {
                assertNoFailures(response);
                Terms terms = response.getAggregations().get("my_terms");
                assertEquals(2, terms.getBuckets().size());
                assertEquals(1, terms.getBucketByKey("foo").getDocCount());
                assertEquals(1, terms.getBucketByKey("bar").getDocCount());
            }, prepareSearch("idx").addAggregation(terms("my_terms").field("str").executionHint(mode.toString()).missing("bar")));
            assertNoFailuresAndResponse(response -> {
                Terms terms = response.getAggregations().get("my_terms");
                assertEquals(1, terms.getBuckets().size());
                assertEquals(2, terms.getBucketByKey("foo").getDocCount());
            }, prepareSearch("idx").addAggregation(terms("my_terms").field("str").missing("foo")));
        }
    }

    public void testLongTerms() {
        assertNoFailuresAndResponse(response -> {
            Terms terms = response.getAggregations().get("my_terms");
            assertEquals(2, terms.getBuckets().size());
            assertEquals(1, terms.getBucketByKey("3").getDocCount());
            assertEquals(1, terms.getBucketByKey("4").getDocCount());
        }, prepareSearch("idx").addAggregation(terms("my_terms").field("long").missing(4)));
        assertNoFailuresAndResponse(response -> {
            assertNoFailures(response);
            Terms terms2 = response.getAggregations().get("my_terms");
            assertEquals(1, terms2.getBuckets().size());
            assertEquals(2, terms2.getBucketByKey("3").getDocCount());
        }, prepareSearch("idx").addAggregation(terms("my_terms").field("long").missing(3)));
    }

    public void testDoubleTerms() {
        assertNoFailuresAndResponse(response -> {
            Terms terms = response.getAggregations().get("my_terms");
            assertEquals(2, terms.getBuckets().size());
            assertEquals(1, terms.getBucketByKey("4.5").getDocCount());
            assertEquals(1, terms.getBucketByKey("5.5").getDocCount());
        }, prepareSearch("idx").addAggregation(terms("my_terms").field("double").missing(4.5)));

        assertNoFailuresAndResponse(response -> {
            Terms terms = response.getAggregations().get("my_terms");
            assertEquals(1, terms.getBuckets().size());
            assertEquals(2, terms.getBucketByKey("5.5").getDocCount());
        }, prepareSearch("idx").addAggregation(terms("my_terms").field("double").missing(5.5)));
    }

    public void testUnmappedHistogram() {
        assertNoFailuresAndResponse(response -> {
            Histogram histogram = response.getAggregations().get("my_histogram");
            assertEquals(1, histogram.getBuckets().size());
            assertEquals(10d, histogram.getBuckets().get(0).getKey());
            assertEquals(2, histogram.getBuckets().get(0).getDocCount());
        }, prepareSearch("idx").addAggregation(histogram("my_histogram").field("non-existing_field").interval(5).missing(12)));
    }

    public void testHistogram() {
        assertNoFailuresAndResponse(response -> {
            Histogram histogram = response.getAggregations().get("my_histogram");
            assertEquals(2, histogram.getBuckets().size());
            assertEquals(0d, histogram.getBuckets().get(0).getKey());
            assertEquals(1, histogram.getBuckets().get(0).getDocCount());
            assertEquals(5d, histogram.getBuckets().get(1).getKey());
            assertEquals(1, histogram.getBuckets().get(1).getDocCount());
        }, prepareSearch("idx").addAggregation(histogram("my_histogram").field("long").interval(5).missing(7)));

        assertNoFailuresAndResponse(response -> {
            Histogram histogram = response.getAggregations().get("my_histogram");
            assertEquals(1, histogram.getBuckets().size());
            assertEquals(0d, histogram.getBuckets().get(0).getKey());
            assertEquals(2, histogram.getBuckets().get(0).getDocCount());
        }, prepareSearch("idx").addAggregation(histogram("my_histogram").field("long").interval(5).missing(3)));
    }

    public void testDateHistogram() {
        assertNoFailuresAndResponse(response -> {
            Histogram histogram = response.getAggregations().get("my_histogram");
            assertEquals(2, histogram.getBuckets().size());
            assertEquals("2014-01-01T00:00:00.000Z", histogram.getBuckets().get(0).getKeyAsString());
            assertEquals(1, histogram.getBuckets().get(0).getDocCount());
            assertEquals("2015-01-01T00:00:00.000Z", histogram.getBuckets().get(1).getKeyAsString());
            assertEquals(1, histogram.getBuckets().get(1).getDocCount());
        },
            prepareSearch("idx").addAggregation(
                dateHistogram("my_histogram").field("date").calendarInterval(DateHistogramInterval.YEAR).missing("2014-05-07")
            )
        );
        assertNoFailuresAndResponse(response -> {
            Histogram histogram = response.getAggregations().get("my_histogram");
            assertEquals(1, histogram.getBuckets().size());
            assertEquals("2015-01-01T00:00:00.000Z", histogram.getBuckets().get(0).getKeyAsString());
            assertEquals(2, histogram.getBuckets().get(0).getDocCount());
        },
            prepareSearch("idx").addAggregation(
                dateHistogram("my_histogram").field("date").calendarInterval(DateHistogramInterval.YEAR).missing("2015-05-07")
            )
        );
    }

    public void testCardinality() {
        assertNoFailuresAndResponse(response -> {
            Cardinality cardinality = response.getAggregations().get("card");
            assertEquals(2, cardinality.getValue());
        }, prepareSearch("idx").addAggregation(cardinality("card").field("long").missing(2)));
    }

    public void testPercentiles() {
        assertNoFailuresAndResponse(response -> {
            Percentiles percentiles = response.getAggregations().get("percentiles");
            assertEquals(1000, percentiles.percentile(100), 0);
        }, prepareSearch("idx").addAggregation(percentiles("percentiles").field("long").missing(1000)));
    }

    public void testStats() {
        assertNoFailuresAndResponse(response -> {
            Stats stats = response.getAggregations().get("stats");
            assertEquals(2, stats.getCount());
            assertEquals(4, stats.getAvg(), 0);
        }, prepareSearch("idx").addAggregation(stats("stats").field("long").missing(5)));
    }

    public void testUnmappedGeoBounds() {
        assertNoFailuresAndResponse(response -> {
            GeoBounds bounds = response.getAggregations().get("bounds");
            assertThat(bounds.bottomRight().lat(), closeTo(2.0, 1E-5));
            assertThat(bounds.bottomRight().lon(), closeTo(1.0, 1E-5));
            assertThat(bounds.topLeft().lat(), closeTo(2.0, 1E-5));
            assertThat(bounds.topLeft().lon(), closeTo(1.0, 1E-5));
        }, prepareSearch("idx").addAggregation(geoBounds("bounds").field("non_existing_field").missing("2,1")));
    }

    public void testGeoBounds() {
        assertNoFailuresAndResponse(response -> {
            GeoBounds bounds = response.getAggregations().get("bounds");
            assertThat(bounds.bottomRight().lat(), closeTo(1.0, 1E-5));
            assertThat(bounds.bottomRight().lon(), closeTo(2.0, 1E-5));
            assertThat(bounds.topLeft().lat(), closeTo(2.0, 1E-5));
            assertThat(bounds.topLeft().lon(), closeTo(1.0, 1E-5));
        }, prepareSearch("idx").addAggregation(geoBounds("bounds").field("location").missing("2,1")));
    }

    public void testGeoCentroid() {
        assertNoFailuresAndResponse(response -> {
            GeoCentroid centroid = response.getAggregations().get("centroid");
            GeoPoint point = new GeoPoint(1.5, 1.5);
            assertThat(point.getY(), closeTo(centroid.centroid().getY(), 1E-5));
            assertThat(point.getX(), closeTo(centroid.centroid().getX(), 1E-5));
        }, prepareSearch("idx").addAggregation(geoCentroid("centroid").field("location").missing("2,1")));
    }
}

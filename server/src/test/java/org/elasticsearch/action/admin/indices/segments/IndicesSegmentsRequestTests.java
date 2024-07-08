/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.util.Collection;

import static org.hamcrest.Matchers.is;

public class IndicesSegmentsRequestTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    @Before
    public void setupIndex() {
        Settings settings = Settings.builder()
            // don't allow any merges so that the num docs is the expected segments
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .build();
        createIndex("test", settings);

        int numDocs = scaledRandomIntBetween(100, 1000);
        for (int j = 0; j < numDocs; ++j) {
            String id = Integer.toString(j);
            prepareIndex("test").setId(id).setSource("text", "sometext").get();
        }
        indicesAdmin().prepareFlush("test").get();
        indicesAdmin().prepareRefresh().get();
    }

    /**
     * with the default IndicesOptions inherited from BroadcastOperationRequest this will raise an exception
     */
    public void testRequestOnClosedIndex() {
        indicesAdmin().prepareClose("test").get();
        try {
            indicesAdmin().prepareSegments("test").get();
            fail("Expected IndexClosedException");
        } catch (IndexClosedException e) {
            assertThat(e.getMessage(), is("closed"));
        }
    }

    /**
     * setting the "ignoreUnavailable" option prevents IndexClosedException
     */
    public void testRequestOnClosedIndexIgnoreUnavailable() {
        indicesAdmin().prepareClose("test").get();
        IndicesOptions defaultOptions = new IndicesSegmentsRequest().indicesOptions();
        IndicesOptions testOptions = IndicesOptions.fromOptions(true, true, true, false, defaultOptions);
        IndicesSegmentResponse rsp = indicesAdmin().prepareSegments("test").setIndicesOptions(testOptions).get();
        assertEquals(0, rsp.getIndices().size());
    }

    /**
     * by default IndicesOptions setting IndicesSegmentsRequest should not throw exception when no index present
     */
    public void testAllowNoIndex() {
        indicesAdmin().prepareDelete("test").get();
        IndicesSegmentResponse rsp = indicesAdmin().prepareSegments().get();
        assertEquals(0, rsp.getIndices().size());
    }

    public void testRequestOnClosedIndexWithVectorFormats() {
        indicesAdmin().prepareClose("test").get();
        try {
            indicesAdmin().prepareSegments("test").includeVectorFormatInfo(true).get();
            fail("Expected IndexClosedException");
        } catch (IndexClosedException e) {
            assertThat(e.getMessage(), is("closed"));
        }
    }

    public void testAllowNoIndexWithVectorFormats() {
        indicesAdmin().prepareDelete("test").get();
        IndicesSegmentResponse rsp = indicesAdmin().prepareSegments().includeVectorFormatInfo(true).get();
        assertEquals(0, rsp.getIndices().size());
    }

    public void testRequestOnClosedIndexIgnoreUnavailableWithVectorFormats() {
        indicesAdmin().prepareClose("test").get();
        IndicesOptions defaultOptions = new IndicesSegmentsRequest().indicesOptions();
        IndicesOptions testOptions = IndicesOptions.fromOptions(true, true, true, false, defaultOptions);
        IndicesSegmentResponse rsp = indicesAdmin().prepareSegments("test")
            .includeVectorFormatInfo(true)
            .setIndicesOptions(testOptions)
            .get();
        assertEquals(0, rsp.getIndices().size());
    }
}

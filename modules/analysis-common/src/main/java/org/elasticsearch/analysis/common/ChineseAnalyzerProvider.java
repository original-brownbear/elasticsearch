/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractConstantAnalyzerProvider;

/**
 * Only for old indexes
 */
public class ChineseAnalyzerProvider extends AbstractConstantAnalyzerProvider<StandardAnalyzer> {

    ChineseAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        // old index: best effort
        super(name, settings, new StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET));
    }
}

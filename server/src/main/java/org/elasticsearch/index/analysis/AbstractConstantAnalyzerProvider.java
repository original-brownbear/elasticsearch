/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.settings.Settings;

public abstract class AbstractConstantAnalyzerProvider<T extends Analyzer> extends AbstractIndexAnalyzerProvider<T> {

    private final T analyzer;

    /**
     * Constructs a new analyzer component, with the index name and its settings and the analyzer name.
     *
     * @param name     The analyzer name
     * @param settings The settings
     * @param analyzer The analyzer
     */
    public AbstractConstantAnalyzerProvider(String name, Settings settings, T analyzer) {
        super(name, settings);
        this.analyzer = analyzer;
    }

    @Override
    public final T get() {
        return analyzer;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.ja.JapaneseCompletionAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractConstantAnalyzerProvider;

public class KuromojiCompletionAnalyzerProvider extends AbstractConstantAnalyzerProvider<JapaneseCompletionAnalyzer> {

    public KuromojiCompletionAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(
            name,
            settings,
            new JapaneseCompletionAnalyzer(
                KuromojiTokenizerFactory.getUserDictionary(env, settings),
                KuromojiCompletionFilterFactory.getMode(settings)
            )
        );
    }
}

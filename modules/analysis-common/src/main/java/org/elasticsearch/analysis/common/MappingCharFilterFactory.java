/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractCharFilterFactory;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.NormalizingCharFilterFactory;

import java.io.Reader;
import java.util.List;
import java.util.regex.Matcher;

public class MappingCharFilterFactory extends AbstractCharFilterFactory implements NormalizingCharFilterFactory {

    private final NormalizeCharMap normMap;

    MappingCharFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);

        List<String> rules = Analysis.getWordList(env, settings, "mappings");
        if (rules == null) {
            throw new IllegalArgumentException("mapping requires either `mappings` or `mappings_path` to be configured");
        }

        NormalizeCharMap.Builder normMapBuilder = new NormalizeCharMap.Builder();
        parseRules(rules, normMapBuilder);
        normMap = normMapBuilder.build();
    }

    @Override
    public Reader create(Reader tokenStream) {
        return new MappingCharFilter(normMap, tokenStream);
    }

    /**
     * parses a list of MappingCharFilter style rules into a normalize char map
     */
    private void parseRules(List<String> rules, NormalizeCharMap.Builder map) {
        for (String rule : rules) {
            Matcher m = WordDelimiterTokenFilterFactory.matchType(rule);
            char[] scratch = new char[256];
            String lhs = WordDelimiterTokenFilterFactory.parseString(m.group(1).trim(), scratch);
            String rhs = WordDelimiterTokenFilterFactory.parseString(m.group(2).trim(), scratch);
            map.add(lhs, rhs);
        }
    }
}

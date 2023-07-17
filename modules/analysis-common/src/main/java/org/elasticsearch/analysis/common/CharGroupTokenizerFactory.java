/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.AttributeFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

import java.util.HashSet;
import java.util.Set;

public class CharGroupTokenizerFactory extends AbstractTokenizerFactory {

    static final String MAX_TOKEN_LENGTH = "max_token_length";

    private final Set<Integer> tokenizeOnChars = new HashSet<>();
    private final Integer maxTokenLength;
    private boolean tokenizeOnSpace = false;
    private boolean tokenizeOnLetter = false;
    private boolean tokenizeOnDigit = false;
    private boolean tokenizeOnPunctuation = false;
    private boolean tokenizeOnSymbol = false;

    public CharGroupTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, settings, name);

        maxTokenLength = settings.getAsInt(MAX_TOKEN_LENGTH, CharTokenizer.DEFAULT_MAX_WORD_LEN);

        for (final String c : settings.getAsList("tokenize_on_chars")) {
            if (c == null || c.length() == 0) {
                throw new RuntimeException("[tokenize_on_chars] cannot contain empty characters");
            }

            int len = c.length();
            if (c.length() == 1) {
                tokenizeOnChars.add((int) c.charAt(0));
            } else if (c.charAt(0) == '\\') {
                char result;
                if (c.charAt(0) == '\\') {
                    char c1 = c.charAt(1);
                    switch (c1) {
                        case '\\' -> result = '\\';
                        case 'n' -> result = '\n';
                        case 't' -> result = '\t';
                        case 'r' -> result = '\r';
                        case 'b' -> result = '\b';
                        case 'f' -> result = '\f';
                        case 'u' -> {
                            if (len > 6) {
                                throw new RuntimeException("Invalid escaped char in [" + c + "]");
                            }
                            result = (char) Integer.parseInt(c.substring(2), 16);
                        }
                        default -> throw new RuntimeException("Invalid escaped char " + c1 + " in [" + c + "]");
                    }
                } else {
                    throw new RuntimeException("Invalid escaped char [" + c + "]");
                }
                tokenizeOnChars.add((int) result);
            } else {
                switch (c) {
                    case "letter" -> tokenizeOnLetter = true;
                    case "digit" -> tokenizeOnDigit = true;
                    case "whitespace" -> tokenizeOnSpace = true;
                    case "punctuation" -> tokenizeOnPunctuation = true;
                    case "symbol" -> tokenizeOnSymbol = true;
                    default -> throw new RuntimeException("Invalid escaped char in [" + c + "]");
                }
            }
        }
    }

    @Override
    public Tokenizer create() {
        return new CharTokenizer(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, maxTokenLength) {
            @Override
            protected boolean isTokenChar(int c) {
                if (tokenizeOnSpace && Character.isWhitespace(c)) {
                    return false;
                }
                if (tokenizeOnLetter && Character.isLetter(c)) {
                    return false;
                }
                if (tokenizeOnDigit && Character.isDigit(c)) {
                    return false;
                }
                if (tokenizeOnPunctuation && CharMatcher.Basic.PUNCTUATION.isTokenChar(c)) {
                    return false;
                }
                if (tokenizeOnSymbol && CharMatcher.Basic.SYMBOL.isTokenChar(c)) {
                    return false;
                }
                return tokenizeOnChars.contains(c) == false;
            }
        };
    }
}

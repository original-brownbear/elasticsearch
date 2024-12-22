/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.HunspellTokenFilterFactory;
import org.elasticsearch.index.analysis.LowercaseNormalizerProvider;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.elasticsearch.index.analysis.PreConfiguredCharFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardAnalyzerProvider;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.wrappers.StableApiWrappers;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.plugins.AnalysisPlugin.requiresAnalysisSettings;

/**
 * Sets up {@link AnalysisRegistry}.
 */
public final class AnalysisModule {

    public static final Map<String, AnalysisProvider<AnalyzerProvider<?>>> NORMALIZERS = Map.of(
        "lowercase",
        (indexSettings, env, name, settings) -> new LowercaseNormalizerProvider(name)
    );

    static {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("_na_").settings(build).build();
        NA_INDEX_SETTINGS = new IndexSettings(metadata, Settings.EMPTY);
    }

    private static final IndexSettings NA_INDEX_SETTINGS;
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(AnalysisModule.class);

    private final HunspellService hunspellService;
    private final AnalysisRegistry analysisRegistry;

    public AnalysisModule(Environment environment, List<AnalysisPlugin> plugins, StablePluginsRegistry stablePluginRegistry)
        throws IOException {
        hunspellService = new HunspellService(environment.settings(), environment, setupHunspellDictionaries(plugins));
        analysisRegistry = new AnalysisRegistry(
            environment,
            setupCharFilters(plugins, stablePluginRegistry),
            setupTokenFilters(plugins, hunspellService, stablePluginRegistry),
            setupTokenizers(plugins, stablePluginRegistry),
            setupAnalyzers(plugins, stablePluginRegistry),
            NORMALIZERS,
            setupPreConfiguredCharFilters(plugins),
            setupPreConfiguredTokenFilters(plugins),
            setupPreConfiguredTokenizers(plugins),
            setupPreBuiltAnalyzerProviderFactories(plugins)
        );
    }

    HunspellService getHunspellService() {
        return hunspellService;
    }

    public AnalysisRegistry getAnalysisRegistry() {
        return analysisRegistry;
    }

    private static Map<String, AnalysisProvider<CharFilterFactory>> setupCharFilters(
        List<AnalysisPlugin> plugins,
        StablePluginsRegistry stablePluginRegistry
    ) {
        NamedRegistry<AnalysisProvider<CharFilterFactory>> charFilters = new NamedRegistry<>("char_filter");
        charFilters.extractAndRegister(plugins, AnalysisPlugin::getCharFilters);
        charFilters.register(StableApiWrappers.oldApiForStableCharFilterFactory(stablePluginRegistry));
        return charFilters.getRegistry();
    }

    public static Map<String, org.apache.lucene.analysis.hunspell.Dictionary> setupHunspellDictionaries(List<AnalysisPlugin> plugins) {
        NamedRegistry<org.apache.lucene.analysis.hunspell.Dictionary> hunspellDictionaries = new NamedRegistry<>("dictionary");
        hunspellDictionaries.extractAndRegister(plugins, AnalysisPlugin::getHunspellDictionaries);
        return hunspellDictionaries.getRegistry();
    }

    private static Map<String, AnalysisProvider<TokenFilterFactory>> setupTokenFilters(
        List<AnalysisPlugin> plugins,
        HunspellService hunspellService,
        StablePluginsRegistry stablePluginRegistry
    ) {
        NamedRegistry<AnalysisProvider<TokenFilterFactory>> tokenFilters = new NamedRegistry<>("token_filter");
        tokenFilters.register("stop", StopTokenFilterFactory::new);
        // Add "standard" for old indices (bwc)
        tokenFilters.register("standard", (indexSettings, environment, name, settings) -> {
            if (indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.V_7_0_0)) {
                throw new IllegalArgumentException("The [standard] token filter has been removed.");
            }
            deprecationLogger.warn(
                DeprecationCategory.ANALYSIS,
                "standard_deprecation",
                "The [standard] token filter name is deprecated and will be removed in a future version."
            );
            return new AbstractTokenFilterFactory(name) {
                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return tokenStream;
                }
            };
        });
        tokenFilters.register("shingle", ShingleTokenFilterFactory::new);
        tokenFilters.register(
            "hunspell",
            requiresAnalysisSettings(
                (indexSettings, env, name, settings) -> new HunspellTokenFilterFactory(name, settings, hunspellService)
            )
        );

        tokenFilters.extractAndRegister(plugins, AnalysisPlugin::getTokenFilters);
        tokenFilters.register(StableApiWrappers.oldApiForTokenFilterFactory(stablePluginRegistry));

        return tokenFilters.getRegistry();
    }

    static Map<String, PreBuiltAnalyzerProviderFactory> setupPreBuiltAnalyzerProviderFactories(List<AnalysisPlugin> plugins) {
        NamedRegistry<PreBuiltAnalyzerProviderFactory> preConfiguredCharFilters = new NamedRegistry<>("pre-built analyzer");
        for (AnalysisPlugin plugin : plugins) {
            for (PreBuiltAnalyzerProviderFactory factory : plugin.getPreBuiltAnalyzerProviderFactories()) {
                preConfiguredCharFilters.register(factory.getName(), factory);
            }
        }
        return unmodifiableMap(preConfiguredCharFilters.getRegistry());
    }

    static Map<String, AnalysisProvider<CharFilterFactory>> setupPreConfiguredCharFilters(List<AnalysisPlugin> plugins) {
        NamedRegistry<PreConfiguredCharFilter> preConfiguredCharFilters = new NamedRegistry<>("pre-configured char_filter");

        // No char filter are available in lucene-core so none are built in to Elasticsearch core

        for (AnalysisPlugin plugin : plugins) {
            for (PreConfiguredCharFilter filter : plugin.getPreConfiguredCharFilters()) {
                preConfiguredCharFilters.register(filter.getName(), filter);
            }
        }
        return unmodifiableMap(preConfiguredCharFilters.getRegistry());
    }

    static Map<String, AnalysisProvider<TokenFilterFactory>> setupPreConfiguredTokenFilters(List<AnalysisPlugin> plugins) {
        NamedRegistry<PreConfiguredTokenFilter> preConfiguredTokenFilters = new NamedRegistry<>("pre-configured token_filter");

        // Add filters available in lucene-core
        preConfiguredTokenFilters.register("lowercase", PreConfiguredTokenFilter.singleton("lowercase", true, LowerCaseFilter::new));
        // Add "standard" for old indices (bwc)
        preConfiguredTokenFilters.register("standard", PreConfiguredTokenFilter.indexVersion("standard", true, (reader, version) -> {
            // This was originally removed in 7_0_0 but due to a cacheing bug it was still possible
            // in certain circumstances to create a new index referencing the standard token filter
            // until version 7_5_2
            if (version.before(IndexVersions.V_7_6_0)) {
                deprecationLogger.warn(
                    DeprecationCategory.ANALYSIS,
                    "standard_deprecation",
                    "The [standard] token filter is deprecated and will be removed in a future version."
                );
            } else {
                throw new IllegalArgumentException("The [standard] token filter has been removed.");
            }
            return reader;
        }));
        /* Note that "stop" is available in lucene-core but it's pre-built
         * version uses a set of English stop words that are in
         * lucene-analyzers-common so "stop" is defined in the analysis-common
         * module. */

        for (AnalysisPlugin plugin : plugins) {
            for (PreConfiguredTokenFilter filter : plugin.getPreConfiguredTokenFilters()) {
                preConfiguredTokenFilters.register(filter.getName(), filter);
            }
        }
        return unmodifiableMap(preConfiguredTokenFilters.getRegistry());
    }

    static Map<String, AnalysisProvider<TokenizerFactory>> setupPreConfiguredTokenizers(List<AnalysisPlugin> plugins) {
        NamedRegistry<PreConfiguredTokenizer> preConfiguredTokenizers = new NamedRegistry<>("pre-configured tokenizer");

        // Temporary shim to register old style pre-configured tokenizers
        final String name = "standard";
        preConfiguredTokenizers.register("name", PreConfiguredTokenizer.singleton(name, StandardTokenizer::new));
        for (AnalysisPlugin plugin : plugins) {
            for (PreConfiguredTokenizer tokenizer : plugin.getPreConfiguredTokenizers()) {
                preConfiguredTokenizers.register(tokenizer.getName(), tokenizer);
            }
        }

        return unmodifiableMap(preConfiguredTokenizers.getRegistry());
    }

    private static Map<String, AnalysisProvider<TokenizerFactory>> setupTokenizers(
        List<AnalysisPlugin> plugins,
        StablePluginsRegistry stablePluginRegistry
    ) {
        NamedRegistry<AnalysisProvider<TokenizerFactory>> tokenizers = new NamedRegistry<>("tokenizer");
        tokenizers.register("standard", StandardTokenizerFactory::new);
        tokenizers.extractAndRegister(plugins, AnalysisPlugin::getTokenizers);
        tokenizers.register(StableApiWrappers.oldApiForTokenizerFactory(stablePluginRegistry));
        return tokenizers.getRegistry();
    }

    private static Map<String, AnalysisProvider<AnalyzerProvider<?>>> setupAnalyzers(
        List<AnalysisPlugin> plugins,
        StablePluginsRegistry stablePluginRegistry
    ) {
        NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> analyzers = new NamedRegistry<>("analyzer");
        analyzers.register("default", StandardAnalyzerProvider::new);
        analyzers.register("standard", StandardAnalyzerProvider::new);
        analyzers.extractAndRegister(plugins, AnalysisPlugin::getAnalyzers);
        analyzers.register(StableApiWrappers.oldApiForAnalyzerFactory(stablePluginRegistry));
        return analyzers.getRegistry();
    }

    /**
     * The basic factory interface for analysis components.
     */
    public interface AnalysisProvider<T> {

        /**
         * Creates a new analysis provider.
         *
         * @param indexSettings the index settings for the index this provider is created for
         * @param environment   the nodes environment to load resources from persistent storage
         * @param name          the name of the analysis component
         * @param settings      the component specific settings without context prefixes
         * @return a new provider instance
         * @throws IOException if an {@link IOException} occurs
         */
        T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException;

        /**
         * Creates a new global scope analysis provider without index specific settings not settings for the provider itself.
         * This can be used to get a default instance of an analysis factory without binding to an index.
         *
         * @param environment the nodes environment to load resources from persistent storage
         * @param name        the name of the analysis component
         * @return a new provider instance
         * @throws IOException              if an {@link IOException} occurs
         * @throws IllegalArgumentException if the provider requires analysis settings ie. if {@link #requiresAnalysisSettings()} returns
         *                                  <code>true</code>
         */
        default T get(Environment environment, String name) throws IOException {
            if (requiresAnalysisSettings()) {
                throw new IllegalArgumentException("Analysis settings required - can't instantiate analysis factory");
            }
            return get(NA_INDEX_SETTINGS, environment, name, NA_INDEX_SETTINGS.getSettings());
        }

        /**
         * If <code>true</code> the analysis component created by this provider requires certain settings to be instantiated.
         * it can't be created with defaults. The default is <code>false</code>.
         */
        default boolean requiresAnalysisSettings() {
            return false;
        }
    }
}

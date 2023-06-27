/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.protocol.xpack;

import java.util.Map;

/**
 * Response object from calling the xpack usage api.
 *
 * Usage information for each feature is accessible through {@link #getUsages()}.
 */
public class XPackUsageResponse {

    private final Map<String, Map<String, Object>> usages;

    private XPackUsageResponse(Map<String, Map<String, Object>> usages) {
        this.usages = usages;
    }

    /** Return a map from feature name to usage information for that feature. */
    public Map<String, Map<String, Object>> getUsages() {
        return usages;
    }

}

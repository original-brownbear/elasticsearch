/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class StaticRestHandler extends BaseRestHandler {

    private final List<Route> routes;

    private final String name;

    private final boolean canTripCircuitBreaker;

    private final Set<String> responseParams;

    protected StaticRestHandler(List<Route> routes, String name) {
        this(routes, name, true, Collections.emptySet());
    }
    protected StaticRestHandler(List<Route> routes, String name, boolean canTripCircuitBreaker) {
        this(routes, name, canTripCircuitBreaker, Collections.emptySet());
    }

    protected StaticRestHandler(List<Route> routes, String name, boolean canTripCircuitBreaker, Set<String> responseParams) {
        this.routes = routes;
        this.name = name;
        this.canTripCircuitBreaker = canTripCircuitBreaker;
        this.responseParams = responseParams;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final List<Route> routes() {
        return routes;
    }

    @Override
    public final boolean canTripCircuitBreaker() {
        return canTripCircuitBreaker;
    }

    @Override
    protected final Set<String> responseParams() {
        return responseParams;
    }
}

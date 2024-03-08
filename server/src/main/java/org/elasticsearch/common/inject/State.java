/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.MatcherAndConverter;

import java.lang.annotation.Annotation;

import static java.util.Collections.emptySet;

/**
 * The inheritable data within an injector. This class is intended to allow parent and local
 * injector data to be accessed as a unit.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
interface State {

    State NONE = new State() {
        @Override
        public State parent() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Scope getScope(Class<? extends Annotation> scopingAnnotation) {
            return null;
        }

        @Override
        public Iterable<MatcherAndConverter> getConvertersThisLevel() {
            return emptySet();
        }

        @Override
        public void blacklist(Key<?> key) {}

        @Override
        public Object lock() {
            throw new UnsupportedOperationException();
        }
    };

    State parent();

    /**
     * Returns the matching scope, or null.
     */
    Scope getScope(Class<? extends Annotation> scopingAnnotation);

    /**
     * Returns all converters at this level only.
     */
    Iterable<MatcherAndConverter> getConvertersThisLevel();

    /**
     * Forbids the corresponding injector from creating a binding to {@code key}. Child injectors
     * blacklist their bound keys on their parent injectors to prevent just-in-time bindings on the
     * parent injector that would conflict.
     */
    void blacklist(Key<?> key);

    /**
     * Returns the shared lock for all injector data. This is a low-granularity, high-contention lock
     * to be used when reading mutable data (ie. just-in-time bindings, and binding blacklists).
     */
    Object lock();

}

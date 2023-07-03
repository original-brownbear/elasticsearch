/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.function.Function;

/**
 * A {@link Function}-like interface which allows throwing checked exceptions.
 */
@FunctionalInterface
public interface CheckedFunction<T, R, E extends Exception> {
    R apply(T t) throws E;

    static <I, EX extends Exception> CheckedFunction<I, I, EX> identity() {
        return in -> in;
    }

    static <I, O, EX extends Exception> CheckedFunction<I, O, EX> toNull() {
        return ignored -> null;
    }

    static <I, O, EX extends Exception> CheckedFunction<I, O, EX> toConstant(O value) {
        return value == null ? toNull() : ignored -> value;
    }
}

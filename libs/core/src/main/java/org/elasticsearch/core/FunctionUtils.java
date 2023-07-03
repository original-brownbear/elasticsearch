/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.core;

import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

public class FunctionUtils {

    private FunctionUtils() {}

    public static final Supplier<Boolean> TRUE_SUPPLIER = () -> true;

    public static final BooleanSupplier TRUE_BOOLEAN_SUPPLIER = () -> true;

    public static final Supplier<Boolean> FALSE_SUPPLIER = () -> false;

    public static final BooleanSupplier FALSE_BOOLEAN_SUPPLIER = () -> false;

    public static BooleanSupplier booleanSupplier(boolean val) {
        return val ? TRUE_BOOLEAN_SUPPLIER : FALSE_BOOLEAN_SUPPLIER;
    }

    public static Supplier<Boolean> constantSupplier(boolean bool) {
        return bool ? TRUE_SUPPLIER : FALSE_SUPPLIER;
    }

    public static <T> Supplier<T> nullSupplier() {
        return () -> null;
    }

    public static <T> Supplier<T> constantSupplier(T value) {
        if (value == null) {
            return nullSupplier();
        }
        return () -> value;
    }

    public static <R, T> Function<R, T> toNull() {
        return ignored -> null;
    }

    public static <R, T> Function<R, T> toConstant(T value) {
        return value == null ? toNull() : ignored -> value;
    }
}

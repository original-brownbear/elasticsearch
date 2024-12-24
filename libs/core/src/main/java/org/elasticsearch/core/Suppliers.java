/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.util.function.Supplier;

public enum Suppliers {
    ;

    @SuppressWarnings("rawtypes")
    private static final Supplier NULL_SUPPLIER = new Supplier() {
        @Override
        public Object get() {
            return null;
        }

        @Override
        public String toString() {
            return "Supplier[null]";
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Supplier<T> nullSupplier() {
        return NULL_SUPPLIER;
    }
}

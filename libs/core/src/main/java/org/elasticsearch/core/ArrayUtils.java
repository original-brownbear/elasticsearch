/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.function.Predicate;

/**
 * Utilities around arrays.
 */
public enum ArrayUtils {
    ;

    public static <T> boolean anyMatch(final T[] array, Predicate<T> predicate) {
        for (T t : array) {
            if (predicate.test(t)) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean contains(final Object[] array, @Nullable final Object val) {
        if (val == null) {
            for (Object o : array) {
                if (o == null) {
                    return true;
                }
            }
        } else {
            for (Object o : array) {
                if (val.equals(o)) {
                    return true;
                }
            }
        }
        return false;
    }
}

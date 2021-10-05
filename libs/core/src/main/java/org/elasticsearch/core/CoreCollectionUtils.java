/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class CoreCollectionUtils {
    /**
     * Deeply inspects a Map, Iterable, or Object array looking for references back to itself.
     * @throws IllegalArgumentException if a self-reference is found
     * @param value The object to evaluate looking for self references
     * @param messageHint A string to be included in the exception message if the call fails, to provide
     *                    more context to the handler of the exception
     */
    public static void ensureNoSelfReferences(Object value, String messageHint) {
        if (value instanceof Map) {
            final Map<?,?> map = (Map<?,?>) value;
            if (map.isEmpty() == false) {
                doEnsureNoSelfReference(map, newIdentitySet(), messageHint);
            }
        } else if ((value instanceof Iterable)) {
            if (isEmptyCollectionOrPath((Iterable<?>) value) == false) {
                doEnsureNoSelfReferences((Iterable<?>) value, value, newIdentitySet(), messageHint);
            }
        } else if (value instanceof Object[]) {
            final Object[] arr = (Object[]) value;
            if (arr.length > 0) {
                doEnsureNoSelfReferences(arr, newIdentitySet(), messageHint);
            }
        }
    }

    public static void ensureNoSelfReferences(Map<String, ?> map, String messageHint) {
        if (map != null && map.isEmpty() == false) {
            doEnsureNoSelfReferences(map.values(), map, newIdentitySet(), messageHint);
        }
    }

    private static Set<Object> newIdentitySet() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
    }

    private static void doEnsureNoSelfReferences(Iterable<?> value, Object originalReference, Set<Object> ancestors,
                                                 String messageHint) {
        safeAddToAncestors(originalReference, ancestors, messageHint);
        for (Object o : value) {
            doEnsureNoSelfReference(o, ancestors, messageHint);
        }
        ancestors.remove(originalReference);
    }

    private static void doEnsureNoSelfReferences(Object[] value, Set<Object> ancestors, String messageHint) {
        safeAddToAncestors(value, ancestors, messageHint);
        for (Object o : value) {
            doEnsureNoSelfReference(o, ancestors, messageHint);
        }
        ancestors.remove(value);
    }

    private static void doEnsureNoSelfReference(Object o, Set<Object> ancestors, String messageHint) {
        if (o instanceof Map) {
            Map<?,?> map = (Map<?,?>) o;
            if (map.isEmpty() == false) {
                doEnsureNoSelfReference(map, ancestors, messageHint);
            }
        } else if ((o instanceof Iterable)) {
            if (isEmptyCollectionOrPath((Iterable<?>) o) == false) {
                doEnsureNoSelfReferences((Iterable<?>) o, o, ancestors, messageHint);
            }
        } else if (o instanceof Object[]) {
            final Object[] arr = (Object[]) o;
            if (arr.length > 0) {
                doEnsureNoSelfReferences(arr, ancestors, messageHint);
            }
        }
    }

    private static void doEnsureNoSelfReference(Map<?, ?> map, Set<Object> ancestors, String messageHint) {
        safeAddToAncestors(map, ancestors, messageHint);
        for (Map.Entry<?, ?> o : map.entrySet()) {
            doEnsureNoSelfReference(o.getKey(), ancestors, messageHint);
            doEnsureNoSelfReference(o.getValue(), ancestors, messageHint);
        }
        ancestors.remove(map);
    }

    private static boolean isEmptyCollectionOrPath(Iterable<?> value) {
        return value instanceof Path || value instanceof Collection && ((Collection<?>) value).isEmpty();
    }

    private static void safeAddToAncestors(Object originalReference, Set<Object> ancestors, String messageHint) {
        if (ancestors.add(originalReference) == false) {
            throwOnSelfReference(messageHint);
        }
    }

    private static void throwOnSelfReference(String messageHint) {
        String suffix = (messageHint == null || messageHint.isEmpty()) ? "" : String.format(Locale.ROOT, " (%s)", messageHint);
        throw new IllegalArgumentException("Iterable object is self-referencing itself" + suffix);
    }
}

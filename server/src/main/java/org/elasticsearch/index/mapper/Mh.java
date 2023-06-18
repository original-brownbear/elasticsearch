/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;

public final class Mh {

    private static final MethodHandles.Lookup lookup = MethodHandles.lookup();

    public static MethodHandle loopConsumers(List<Class<?>> classes, String methodName, Class<?> argType) {
        var methodType = MethodType.methodType(void.class, argType);
        List<MethodHandle> handles = classes.stream().map(cls -> {
            try {
                return lookup.findVirtual(cls, methodName, methodType);
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }).toList();
        var res = MethodHandles.empty(MethodType.methodType(void.class));
        int[] reorder = new int[classes.size() * 2];
        for (int i = 0; i < classes.size(); i++) {
            res = MethodHandles.collectArguments(res, 2 * i, handles.get(classes.size() - i - 1));
            reorder[2 * i] = classes.size() - i;
        }
        res = MethodHandles.permuteArguments(res, MethodType.methodType(void.class, argType, classes.toArray(Class[]::new)), reorder);
        return res.asSpreader(Object[].class, classes.size());
    }
}
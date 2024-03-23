/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

public class AnnotationTestObject {

    public record TestAnnotation(String one, String two, String three) {
        public static final String NAME = "test_annotation";
    }

    public void deprecatedMethod() {

    }

    public void annotatedTestMethod() {

    }

    public void annotatedMultipleMethod() {

    }
}

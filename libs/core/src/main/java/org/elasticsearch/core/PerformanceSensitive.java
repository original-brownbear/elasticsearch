/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

/**
 * Indicates that the annotated element is performance sensitive. Changes to annotated element should not introduce overhead without good
 * justification and should not be done without an understanding of their performance implications in general.
 * The value of {@link #value} my provide further detail on why the element is annotated.
 */
public @interface PerformanceSensitive {

    String value() default "";
}

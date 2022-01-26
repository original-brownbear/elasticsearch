/*
 * Copyright (C) 2009 Google Inc.
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

package org.elasticsearch.common.inject.spi;

import org.elasticsearch.common.inject.TypeLiteral;

/**
 * Context of an injectable type encounter. Enables reporting errors, registering injection
 * listeners and binding method interceptors for injectable type {@code I}. It is an error to use
 * an encounter after the {@link TypeListener#hear(TypeLiteral, TypeEncounter) hear()} method has
 * returned.
 *
 * @param <I> the injectable type encountered
 * @since 2.0
 */
@SuppressWarnings("overloads")
public interface TypeEncounter<I> {

}

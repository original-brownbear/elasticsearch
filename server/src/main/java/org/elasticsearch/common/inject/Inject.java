/*
 * Copyright (C) 2006 Google Inc.
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

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotates members of your implementation class (constructors, methods
 * and fields) into which the {@link Injector} should inject values.
 * The Injector fulfills injection requests for:
 * <ul>
 * <li>Every instance it constructs. The class being constructed must have
 * exactly one of its constructors marked with {@code @Inject} or must have a
 * constructor taking no parameters. The Injector then proceeds to perform
 * method and field injections.
 * <li>Pre-constructed instances passed to
 * {@link org.elasticsearch.common.inject.binder.LinkedBindingBuilder#toInstance(Object)} and
 * {@link org.elasticsearch.common.inject.binder.LinkedBindingBuilder#toProvider(Provider)}.
 * In this case all constructors are, of course, ignored.
 * </ul>
 * <p>
 * In all cases, a member can be injected regardless of its Java access
 * specifier (private, default, protected, public).
 *
 * @author crazybob@google.com (Bob Lee)
 */
@Target({ METHOD, CONSTRUCTOR, FIELD })
@Retention(RUNTIME)
@Documented
public @interface Inject {
}

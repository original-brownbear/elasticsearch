/*
 * Copyright (C) 2008 Google Inc.
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

/**
 * A module whose configuration information is hidden from its environment by default. Only bindings
 * that are explicitly exposed will be available to other modules and to the users of the injector.
 * This module may expose the bindings it creates and the bindings of the modules it installs.
 * <p>
 * A private module can be nested within a regular module or within another private module using
 * {@link Binder#install install()}.  Its bindings live in a new environment that inherits bindings,
 * type converters, scopes, and interceptors from the surrounding ("parent") environment.  When you
 * nest multiple private modules, the result is a tree of environments where the injector's
 * environment is the root.
 * <p>
 * {@literal @}{@link org.elasticsearch.common.inject.Provides Provides} bindings can be exposed with the {@literal @}{@link
 * Exposed} annotation:
 * <pre>
 * public class FooBarBazModule extends PrivateModule {
 *   protected void configure() {
 *     bind(Foo.class).to(RealFoo.class);
 *     expose(Foo.class);
 *
 *     install(new TransactionalBarModule());
 *     expose(Bar.class).annotatedWith(Transactional.class);
 *
 *     bind(SomeImplementationDetail.class);
 *     install(new MoreImplementationDetailsModule());
 *   }
 *
 *   {@literal @}Provides {@literal @}Exposed
 *   public Baz provideBaz() {
 *     return new SuperBaz();
 *   }
 * }
 * </pre>
 * <p>
 * The scope of a binding is constrained to its environment. A singleton bound in a private
 * module will be unique to its environment. But a binding for the same type in a different private
 * module will yield a different instance.
 * <p>
 * A shared binding that injects the {@code Injector} gets the root injector, which only has
 * access to bindings in the root environment. An explicit binding that injects the {@code Injector}
 * gets access to all bindings in the child environment.
 * <p>
 * To promote a just-in-time binding to an explicit binding, bind it:
 * <pre>
 *   bind(FooImpl.class);
 * </pre>
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public abstract class PrivateModule implements Module {

    /**
     * Like abstract module, the binder of the current private module
     */
    private PrivateBinder binder;

    @Override
    public final synchronized void configure(Binder binder) {
        if (this.binder != null) {
            throw new IllegalStateException("Re-entry is not allowed.");
        }

        // Guice treats PrivateModules specially and passes in a PrivateBinder automatically.
        this.binder = (PrivateBinder) binder.skipSources(PrivateModule.class);
        try {
            configure();
        } finally {
            this.binder = null;
        }
    }

    /**
     * Creates bindings and other configurations private to this module.
     */
    protected abstract void configure();

    // everything below is copied from AbstractModule

}

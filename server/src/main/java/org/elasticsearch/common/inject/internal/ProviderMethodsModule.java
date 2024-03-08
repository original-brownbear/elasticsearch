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

package org.elasticsearch.common.inject.internal;

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.util.Modules;

import java.util.Objects;

/**
 * Creates bindings to methods annotated with {@literal @}. Use the scope and
 * binding annotations on the provider method to configure the binding.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 */
public final class ProviderMethodsModule implements Module {
    private final Object delegate;

    private ProviderMethodsModule(Object delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * Returns a module which creates bindings for provider methods from the given module.
     */
    public static Module forModule(Module module) {
        return forObject(module);
    }

    /**
     * Returns a module which creates bindings for provider methods from the given object.
     * This is useful notably for <a href="http://code.google.com/p/google-gin/">GIN</a>
     */
    public static Module forObject(Object object) {
        // avoid infinite recursion, since installing a module always installs itself
        if (object instanceof ProviderMethodsModule) {
            return Modules.EMPTY_MODULE;
        }

        return new ProviderMethodsModule(object);
    }

    @Override
    public void configure(Binder binder) {}

    @Override
    public boolean equals(Object o) {
        return o instanceof ProviderMethodsModule && ((ProviderMethodsModule) o).delegate == delegate;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}

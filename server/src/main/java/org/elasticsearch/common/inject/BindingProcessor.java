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

import org.elasticsearch.common.inject.internal.BindingImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.UntargettedBindingImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Handles {@link Binder#bind} elements.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 */
class BindingProcessor extends AbstractProcessor {

    private final List<CreationListener> creationListeners = new ArrayList<>();
    private final Initializer initializer;
    private final List<Runnable> uninitializedBindings = new ArrayList<>();

    BindingProcessor(Errors errors, Initializer initializer) {
        super(errors);
        this.initializer = initializer;
    }

    static <T> UntargettedBindingImpl<T> invalidBinding(InjectorImpl injector, Key<T> key, Object source) {
        return new UntargettedBindingImpl<>(injector, key, source);
    }

    public void initializeBindings() {
        for (Runnable initializer : uninitializedBindings) {
            initializer.run();
        }
    }

    public void runCreationListeners() {
        for (CreationListener creationListener : creationListeners) {
            creationListener.notify(errors);
        }
    }

    private void putBinding(BindingImpl<?> binding) {
        Key<?> key = binding.getKey();

        Class<?> rawType = key.getRawType();
        if (FORBIDDEN_TYPES.contains(rawType)) {
            errors.cannotBindToGuiceType(rawType.getSimpleName());
            return;
        }

        Binding<?> original = injector.state.getExplicitBinding(key);
        if (original != null) {
            errors.bindingAlreadySet(key, original.getSource());
            return;
        }

        // prevent the parent from creating a JIT binding for this key
        injector.state.parent().blacklist(key);
        injector.state.putBinding(key, binding);
    }

    // It's unfortunate that we have to maintain a blacklist of specific
    // classes, but we can't easily block the whole package because of
    // all our unit tests.
    private static final Set<Class<?>> FORBIDDEN_TYPES = Set.of(
        AbstractModule.class,
        Binder.class,
        Binding.class,
        Injector.class,
        Key.class,
        MembersInjector.class,
        Module.class,
        Provider.class,
        Scope.class,
        TypeLiteral.class
    );
    // TODO(jessewilson): fix BuiltInModule, then add Stage

    interface CreationListener {
        void notify(Errors errors);
    }
}

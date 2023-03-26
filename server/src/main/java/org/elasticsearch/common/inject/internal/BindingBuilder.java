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
import org.elasticsearch.common.inject.ConfigurationException;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.binder.AnnotatedBindingBuilder;
import org.elasticsearch.common.inject.spi.Element;
import org.elasticsearch.common.inject.spi.InjectionPoint;
import org.elasticsearch.common.inject.spi.InstanceBinding;
import org.elasticsearch.common.inject.spi.Message;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Bind a non-constant key.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
public class BindingBuilder<T> implements AnnotatedBindingBuilder<T> {

    public static final String IMPLEMENTATION_ALREADY_SET = "Implementation is set more than once.";
    public static final String SINGLE_INSTANCE_AND_SCOPE = "Setting the scope is not permitted when binding to a single instance.";
    public static final String SCOPE_ALREADY_SET = "Scope is set more than once.";
    public static final String BINDING_TO_NULL = "Binding to null instances is not allowed. "
        + "Use toProvider(Providers.of(null)) if this is your intended behaviour.";

    protected final List<Element> elements;
    protected final int position;
    protected final Binder binder;
    private BindingImpl<T> binding;

    public BindingBuilder(Binder binder, List<Element> elements, Object source, Key<T> key) {
        this.binder = binder;
        this.elements = elements;
        this.position = elements.size();
        this.binding = new UntargettedBindingImpl<>(source, key, Scoping.UNSCOPED);
        elements.add(position, this.binding);
    }

    @Override
    public BindingBuilder<T> to(Class<? extends T> implementation) {
        Key<? extends T> linkedKey = Key.get(implementation);
        Objects.requireNonNull(linkedKey, "linkedKey");
        checkNotTargetted();
        BindingImpl<T> base = getBinding();
        setBinding(new LinkedBindingImpl<>(base.getSource(), base.getKey(), base.getScoping(), linkedKey));
        return this;
    }

    @Override
    public void toInstance(T instance) {
        checkNotTargetted();

        // lookup the injection points, adding any errors to the binder's errors list
        Set<InjectionPoint> injectionPoints;
        if (instance != null) {
            try {
                injectionPoints = InjectionPoint.forInstanceMethodsAndFields(instance.getClass());
            } catch (ConfigurationException e) {
                for (Message message : e.getErrorMessages()) {
                    binder.addError(message);
                }
                injectionPoints = Set.copyOf(e.getPartialValue());
            }
        } else {
            binder.addError(BINDING_TO_NULL);
            injectionPoints = emptySet();
        }

        BindingImpl<T> base = getBinding();
        setBinding(new InstanceBindingImpl<>(base.getSource(), base.getKey(), base.getScoping(), injectionPoints, instance));
    }

    @Override
    public BindingBuilder<T> toProvider(Provider<? extends T> provider) {
        Objects.requireNonNull(provider, "provider");
        checkNotTargetted();

        // lookup the injection points, adding any errors to the binder's errors list
        Set<InjectionPoint> injectionPoints;
        try {
            injectionPoints = InjectionPoint.forInstanceMethodsAndFields(provider.getClass());
        } catch (ConfigurationException e) {
            for (Message message : e.getErrorMessages()) {
                binder.addError(message);
            }
            injectionPoints = Set.copyOf(e.getPartialValue());
        }

        BindingImpl<T> base = getBinding();
        setBinding(new ProviderInstanceBindingImpl<>(base.getSource(), base.getKey(), base.getScoping(), injectionPoints, provider));
        return this;
    }

    @Override
    public String toString() {
        return "BindingBuilder<" + getBinding().getKey().getTypeLiteral() + ">";
    }

    protected BindingImpl<T> getBinding() {
        return binding;
    }

    protected BindingImpl<T> setBinding(BindingImpl<T> binding) {
        this.binding = binding;
        elements.set(position, binding);
        return binding;
    }

    public void in(final Class<? extends Annotation> scopeAnnotation) {
        Objects.requireNonNull(scopeAnnotation, "scopeAnnotation");
        checkNotScoped();
        setBinding(getBinding().withScoping(Scoping.forAnnotation(scopeAnnotation)));
    }

    public void asEagerSingleton() {
        checkNotScoped();
        setBinding(getBinding().withScoping(Scoping.EAGER_SINGLETON));
    }

    protected void checkNotTargetted() {
        if ((binding instanceof UntargettedBindingImpl) == false) {
            binder.addError(IMPLEMENTATION_ALREADY_SET);
        }
    }

    protected void checkNotScoped() {
        // Scoping isn't allowed when we have only one instance.
        if (binding instanceof InstanceBinding) {
            binder.addError(SINGLE_INSTANCE_AND_SCOPE);
            return;
        }

        if (binding.getScoping().isExplicitlyScoped()) {
            binder.addError(SCOPE_ALREADY_SET);
        }
    }
}

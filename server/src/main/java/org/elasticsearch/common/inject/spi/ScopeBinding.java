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

package org.elasticsearch.common.inject.spi;

import java.util.Objects;

/**
 * Registration of a scope annotation with the scope that implements it. Instances are created
 * explicitly in a module using {@link org.elasticsearch.common.inject.Binder#bindScope() bindScope()}
 * statements:
 * <pre>
 *     Scope recordScope = new RecordScope();
 *     bindScope(RecordScoped.class, new RecordScope());</pre>
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class ScopeBinding implements Element {
    private final Object source;

    ScopeBinding(Object source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public Object getSource() {
        return source;
    }

    @Override
    public <T> T acceptVisitor(ElementVisitor<T> visitor) {
        return visitor.visitScope();
    }

}

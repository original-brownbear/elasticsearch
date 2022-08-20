/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

public interface Parameter<T> extends Supplier<T> {
    /**
     * Defines a parameter that takes the values {@code true} or {@code false}
     *
     * @param name         the parameter name
     * @param updateable   whether the parameter can be changed by a mapping update
     * @param initializer  a function that reads the parameter value from an existing mapper
     * @param defaultValue the default value, to be used if the parameter is undefined in a mapping
     */
    static Parameter<Boolean> boolParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Boolean> initializer,
            boolean defaultValue
    ) {
        return new FieldMapper.ParameterImpl<>(
                name,
                updateable,
                defaultValue ? () -> true : () -> false,
                (n, c, o) -> XContentMapValues.nodeBooleanValue(o),
                initializer,
                XContentBuilder::field,
                Objects::toString
        );
    }

    /**
     * Defines a parameter that takes the values {@code true} or {@code false}, and will always serialize
     * its value if configured.
     *
     * @param name         the parameter name
     * @param updateable   whether the parameter can be changed by a mapping update
     * @param initializer  a function that reads the parameter value from an existing mapper
     * @param defaultValue the default value, to be used if the parameter is undefined in a mapping
     */
    static Parameter<Explicit<Boolean>> explicitBoolParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Explicit<Boolean>> initializer,
            boolean defaultValue
    ) {
        return new FieldMapper.ParameterImpl<>(
                name,
                updateable,
                defaultValue ? () -> Explicit.IMPLICIT_TRUE : () -> Explicit.IMPLICIT_FALSE,
                (n, c, o) -> Explicit.explicitBoolean(XContentMapValues.nodeBooleanValue(o)),
                initializer,
                (b, n, v) -> b.field(n, v.value()),
                v -> Boolean.toString(v.value())
        );
    }

    /**
     * Defines a parameter that takes an integer value
     *
     * @param name         the parameter name
     * @param updateable   whether the parameter can be changed by a mapping update
     * @param initializer  a function that reads the parameter value from an existing mapper
     * @param defaultValue the default value, to be used if the parameter is undefined in a mapping
     */
    static Parameter<Integer> intParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Integer> initializer,
            int defaultValue
    ) {
        return new FieldMapper.ParameterImpl<>(
                name,
                updateable,
                () -> defaultValue,
                (n, c, o) -> XContentMapValues.nodeIntegerValue(o),
                initializer,
                XContentBuilder::field,
                Objects::toString
        );
    }

    /**
     * Defines a parameter that takes a string value
     *
     * @param name         the parameter name
     * @param updateable   whether the parameter can be changed by a mapping update
     * @param initializer  a function that reads the parameter value from an existing mapper
     * @param defaultValue the default value, to be used if the parameter is undefined in a mapping
     */
    static Parameter<String> stringParam(
            String name,
            boolean updateable,
            Function<FieldMapper, String> initializer,
            String defaultValue
    ) {
        return stringParam(name, updateable, initializer, defaultValue, XContentBuilder::field);
    }

    static Parameter<String> stringParam(
            String name,
            boolean updateable,
            Function<FieldMapper, String> initializer,
            String defaultValue,
            FieldMapper.Serializer<String> serializer
    ) {
        return new FieldMapper.ParameterImpl<>(
                name,
                updateable,
                defaultValue == null ? () -> null : () -> defaultValue,
                (n, c, o) -> XContentMapValues.nodeStringValue(o),
                initializer,
                serializer,
                Function.identity()
        );
    }

    @SuppressWarnings("unchecked")
    static Parameter<List<String>> stringArrayParam(
            String name,
            boolean updateable,
            Function<FieldMapper, List<String>> initializer
    ) {
        return new FieldMapper.ParameterImpl<>(name, updateable, List::of, (n, c, o) -> {
            List<Object> values = (List<Object>) o;
            List<String> strValues = new ArrayList<>();
            for (Object item : values) {
                strValues.add(item.toString());
            }
            return strValues;
        }, initializer, XContentBuilder::stringListField, Objects::toString);
    }

    /**
     * Defines a parameter that takes any of the values of an enumeration.
     *
     * @param name         the parameter name
     * @param updateable   whether the parameter can be changed by a mapping update
     * @param initializer  a function that reads the parameter value from an existing mapper
     * @param defaultValue the default value, to be used if the parameter is undefined in a mapping
     * @param enumClass    the enumeration class the parameter takes values from
     */
    static <T extends Enum<T>> Parameter<T> enumParam(
            String name,
            boolean updateable,
            Function<FieldMapper, T> initializer,
            T defaultValue,
            Class<T> enumClass
    ) {
        Set<T> acceptedValues = EnumSet.allOf(enumClass);
        return restrictedEnumParam(name, updateable, initializer, defaultValue, enumClass, acceptedValues);
    }

    /**
     * Defines a parameter that takes one of a restricted set of values from an enumeration.
     *
     * @param name         the parameter name
     * @param updateable   whether the parameter can be changed by a mapping update
     * @param initializer  a function that reads the parameter value from an existing mapper
     * @param defaultValue the default value, to be used if the parameter is undefined in a mapping
     * @param enumClass    the enumeration class the parameter takes values from
     * @param values       the set of values that the parameter can take
     */
    static <T extends Enum<T>> Parameter<T> restrictedEnumParam(
            String name,
            boolean updateable,
            Function<FieldMapper, T> initializer,
            T defaultValue,
            Class<T> enumClass,
            Set<T> values
    ) {
        assert values.size() > 0;
        return new FieldMapper.ParameterImpl<T>(name, updateable, () -> defaultValue, (n, c, o) -> {
            if (o == null) {
                return defaultValue;
            }
            try {
                @SuppressWarnings("unchecked")
                T enumValue = Enum.valueOf(enumClass, (String) o);
                return enumValue;
            } catch (IllegalArgumentException e) {
                throw new MapperParsingException("Unknown value [" + o + "] for field [" + name + "] - accepted values are " + values);
            }
        }, initializer, XContentBuilder::field, Objects::toString).addValidator(v -> {
            if (v != null && values.contains(v) == false) {
                throw new MapperParsingException("Unknown value [" + v + "] for field [" + name + "] - accepted values are " + values);
            }
        });
    }

    /**
     * Defines a parameter that takes an analyzer name
     *
     * @param name                the parameter name
     * @param updateable          whether the parameter can be changed by a mapping update
     * @param initializer         a function that reads the parameter value from an existing mapper
     * @param defaultAnalyzer     the default value, to be used if the parameter is undefined in a mapping
     * @param indexCreatedVersion the version on which this index was created
     */
    static Parameter<NamedAnalyzer> analyzerParam(
            String name,
            boolean updateable,
            Function<FieldMapper, NamedAnalyzer> initializer,
            Supplier<NamedAnalyzer> defaultAnalyzer,
            Version indexCreatedVersion
    ) {
        return new FieldMapper.ParameterImpl<>(name, updateable, defaultAnalyzer, (n, c, o) -> {
            String analyzerName = o.toString();
            NamedAnalyzer a = c.getIndexAnalyzers().get(analyzerName);
            if (a == null) {
                if (indexCreatedVersion.isLegacyIndexVersion()) {
                    FieldMapper.logger.warn(() -> format("Could not find analyzer [%s] of legacy index, falling back to default", analyzerName));
                    a = defaultAnalyzer.get();
                } else {
                    throw new IllegalArgumentException("analyzer [" + analyzerName + "] has not been configured in mappings");
                }
            }
            return a;
        }, initializer, (b, n, v) -> b.field(n, v.name()), NamedAnalyzer::name);
    }

    /**
     * Defines a parameter that takes an analyzer name
     *
     * @param name            the parameter name
     * @param updateable      whether the parameter can be changed by a mapping update
     * @param initializer     a function that reads the parameter value from an existing mapper
     * @param defaultAnalyzer the default value, to be used if the parameter is undefined in a mapping
     */
    static Parameter<NamedAnalyzer> analyzerParam(
            String name,
            boolean updateable,
            Function<FieldMapper, NamedAnalyzer> initializer,
            Supplier<NamedAnalyzer> defaultAnalyzer
    ) {
        return analyzerParam(name, updateable, initializer, defaultAnalyzer, Version.CURRENT);
    }

    /**
     * Declares a metadata parameter
     */
    static Parameter<Map<String, String>> metaParam() {
        return new FieldMapper.ParameterImpl<>(
                "meta",
                true,
                Collections::emptyMap,
                (n, c, o) -> TypeParsers.parseMeta(n, o),
                m -> m.fieldType().meta(),
                XContentBuilder::stringStringMap,
                Objects::toString
        );
    }

    static Parameter<Boolean> indexParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
        return Parameter.boolParam("index", false, initializer, defaultValue);
    }

    static Parameter<Boolean> storeParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
        return Parameter.boolParam("store", false, initializer, defaultValue);
    }

    static Parameter<Boolean> docValuesParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
        return Parameter.boolParam("doc_values", false, initializer, defaultValue);
    }

    /**
     * Defines a script parameter
     *
     * @param initializer retrieves the equivalent parameter from an existing FieldMapper for use in merges
     * @return a script parameter
     */
    static Parameter<Script> scriptParam(Function<FieldMapper, Script> initializer) {
        return new FieldMapper.ParameterImpl<>("script", false, () -> null, (n, c, o) -> {
            if (o == null) {
                return null;
            }
            Script script = Script.parse(o);
            if (script.getType() == ScriptType.STORED) {
                throw new IllegalArgumentException("stored scripts are not supported on field [" + n + "]");
            }
            return script;
        }, initializer, XContentBuilder::field, Objects::toString).acceptsNull();
    }

    /**
     * Defines an on_script_error parameter
     *
     * @param initializer          retrieves the equivalent parameter from an existing FieldMapper for use in merges
     * @param dependentScriptParam the corresponding required script parameter
     * @return a new on_error_script parameter
     */
    static Parameter<String> onScriptErrorParam(
            Function<FieldMapper, String> initializer,
            Parameter<Script> dependentScriptParam
    ) {
        return new FieldMapper.ParameterImpl<>(
                "on_script_error",
                true,
                () -> "fail",
                (n, c, o) -> XContentMapValues.nodeStringValue(o),
                initializer,
                XContentBuilder::field,
                Function.identity()
        ).addValidator(v -> {
            switch (v) {
                case "fail":
                case "continue":
                    return;
                default:
                    throw new MapperParsingException(
                            "Unknown value [" + v + "] for field [on_script_error] - accepted values are [fail, continue]"
                    );
            }
        }).requiresParameter(dependentScriptParam);
    }

    boolean isDeprecated();

    String name();

    T getValue();

    @Override
    T get();

    T getDefaultValue();

    void setValue(T value);

    boolean isConfigured();

    Parameter<T> acceptsNull();

    boolean canAcceptNull();

    Parameter<T> addDeprecatedName(String deprecatedName);

    Parameter<T> deprecated();

    List<String> deprecatedNames();

    Parameter<T> addValidator(Consumer<T> validator);

    Parameter<T> setSerializerCheck(FieldMapper.SerializerCheck<T> check);

    Parameter<T> alwaysSerialize();

    Parameter<T> neverSerialize();

    Parameter<T> setMergeValidator(FieldMapper.MergeValidator<T> mergeValidator);

    Parameter<T> requiresParameter(Parameter<?> ps);

    Parameter<T> precludesParameters(Parameter<?>... ps);

    void validate();

    void init(FieldMapper toInit);

    void parse(String field, MappingParserContext context, Object in);

    void merge(FieldMapper toMerge, FieldMapper.Conflicts conflicts);

    void toXContent(XContentBuilder builder, boolean includeDefaults) throws IOException;
}

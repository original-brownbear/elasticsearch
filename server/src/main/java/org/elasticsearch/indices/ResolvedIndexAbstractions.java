package org.elasticsearch.indices;

import org.elasticsearch.cluster.metadata.IndexAbstraction;

import java.util.Map;
import java.util.function.Predicate;

public final class ResolvedIndexAbstractions {

    private final Predicate<IndexAbstraction> predicate;

    private final Map<String, IndexAbstraction> lookup;

    public ResolvedIndexAbstractions(Predicate<IndexAbstraction> predicate, Map<String, IndexAbstraction> lookup) {
        this.predicate = predicate;
        this.lookup = lookup;
    }

    public boolean authorized(String index) {
        final IndexAbstraction indexAbstraction = lookup.get(index);
        assert indexAbstraction != null;
        return predicate.test(indexAbstraction);
    }

    public boolean empty() {
        return lookup.isEmpty();
    }
}

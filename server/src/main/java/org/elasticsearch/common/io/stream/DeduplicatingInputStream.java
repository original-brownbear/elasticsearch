package org.elasticsearch.common.io.stream;

import java.io.IOException;

public class DeduplicatingInputStream extends NamedWriteableAwareStreamInput {

    private final ObjectDeduplicatorService deduplicatorService;

    public DeduplicatingInputStream(StreamInput delegate, NamedWriteableRegistry namedWriteableRegistry,
                                    ObjectDeduplicatorService deduplicatorService) {
        super(delegate, namedWriteableRegistry);
        this.deduplicatorService = deduplicatorService;
    }

    public <T> T read(Writeable.Reader<T> reader) throws IOException {
        return deduplicatorService.read(reader, this);
    }
}

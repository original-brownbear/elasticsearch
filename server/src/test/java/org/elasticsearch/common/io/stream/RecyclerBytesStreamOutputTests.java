/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for {@link StreamOutput}.
 */
public class RecyclerBytesStreamOutputTests extends AbstractBytesStreamOutputTests {

    private final Recycler<BytesRef> recycler = new BytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE);

    @Override
    protected BytesStream newStream() {
        return new RecyclerBytesStreamOutput(recycler);
    }

    public void testOverflow() {
        final var pageSize = randomFrom(ByteSizeUnit.MB.toIntBytes(1L), ByteSizeUnit.KB.toIntBytes(16)) + between(-1024, 1024);
        final var pagesAllocated = new AtomicLong();

        try (RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(new Recycler<>() {
            private final V<BytesRef> page = new V<>() {
                private final BytesRef bytesRef = new BytesRef(new byte[pageSize]);

                @Override
                public BytesRef v() {
                    return bytesRef;
                }

                @Override
                public boolean isRecycled() {
                    return false;
                }

                @Override
                public void close() {
                    pagesAllocated.decrementAndGet();
                }
            };

            @Override
            public V<BytesRef> obtain() {
                pagesAllocated.incrementAndGet();
                return page;
            }
        })) {
            var bytesAllocated = 0;
            while (bytesAllocated < Integer.MAX_VALUE) {
                var thisAllocation = between(1, Integer.MAX_VALUE - bytesAllocated);
                bytesAllocated += thisAllocation;
                final long expectedPages = (long) bytesAllocated / pageSize + (bytesAllocated % pageSize == 0 ? 0 : 1);
                try {
                    output.skip(thisAllocation);
                    assertThat(pagesAllocated.get(), equalTo(expectedPages));
                } catch (IllegalArgumentException e) {
                    assertThat(expectedPages * pageSize, greaterThan((long) Integer.MAX_VALUE));
                    return;
                }
            }
        } finally {
            assertThat(pagesAllocated.get(), equalTo(0L));
        }
    }
}

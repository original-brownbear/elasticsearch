package org.elasticsearch.xpack.searchablesnapshots.cache;

import java.util.Objects;

public final class ByteRange {

    public static final ByteRange EMPTY = new ByteRange(0L, 0L);

    private final long start;

    private final long end;

    public static ByteRange of(long start, long end) {
        return new ByteRange(start, end);
    }

    private ByteRange(long start, long end) {
        this.start = start;
        this.end = end;
        assert end >= start : "End must be greater or equal to start but saw [" + start + "][" + start + "]";
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ByteRange == false) {
            return false;
        }
        final ByteRange that = (ByteRange) obj;
        return start == that.start && end == that.end;
    }
}

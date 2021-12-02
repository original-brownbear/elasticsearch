package org.elasticsearch.http;

import java.io.IOException;
import java.io.OutputStream;

public interface ChunkedHttpBody {

    boolean serialize(OutputStream out, int sizeHint) throws IOException;
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * Snapshot metadata file format used in v2.0 and above
 */
public final class ChecksumBlobStoreFormat<T extends ToXContent> {

    // Serialization parameters to specify correct context for metadata serialization
    public static final ToXContent.Params SNAPSHOT_ONLY_FORMAT_PARAMS;

    static {
        Map<String, String> snapshotOnlyParams = new HashMap<>();
        // when metadata is serialized certain elements of the metadata shouldn't be included into snapshot
        // exclusion of these elements is done by setting Metadata.CONTEXT_MODE_PARAM to Metadata.CONTEXT_MODE_SNAPSHOT
        snapshotOnlyParams.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_SNAPSHOT);
        // serialize SnapshotInfo using the SNAPSHOT mode
        snapshotOnlyParams.put(SnapshotInfo.CONTEXT_MODE_PARAM, SnapshotInfo.CONTEXT_MODE_SNAPSHOT);
        SNAPSHOT_ONLY_FORMAT_PARAMS = new ToXContent.MapParams(snapshotOnlyParams);
    }

    // The format version
    public static final int VERSION = 1;

    private static final int BUFFER_SIZE = 4096;

    private final String codec;

    private final String blobNameFormat;

    private final CheckedFunction<XContentParser, T, IOException> reader;

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     */
    public ChecksumBlobStoreFormat(String codec, String blobNameFormat, CheckedFunction<XContentParser, T, IOException> reader) {
        this.reader = reader;
        this.blobNameFormat = blobNameFormat;
        this.codec = codec;
    }

    /**
     * Reads and parses the blob with given name, applying name translation using the {link #blobName} method
     *
     * @param blobContainer blob container
     * @param name          name to be translated into
     * @return parsed blob object
     */
    public T read(BlobContainer blobContainer, String name, NamedXContentRegistry namedXContentRegistry) throws IOException {
        String blobName = blobName(name);
        try (InputStream in = blobContainer.readBlob(blobName)) {
            return deserialize(namedXContentRegistry, in);
        }
    }

    public String blobName(String name) {
        return String.format(Locale.ROOT, blobNameFormat, name);
    }

    public T deserialize(NamedXContentRegistry namedXContentRegistry, InputStream inputStream) throws IOException {
        try {
            CodecUtil.checkHeader(new InputStreamDataInput(inputStream), codec, VERSION, VERSION);
            if (inputStream.markSupported() == false) {
                inputStream = new BufferedInputStream(inputStream);
            }
            inputStream.mark(4);
            final byte[] maybeCompressorHeader = new byte[4];
            Streams.readFully(inputStream, maybeCompressorHeader);
            boolean compressed = CompressorFactory.COMPRESSOR.isCompressed(new BytesArray(maybeCompressorHeader));
            inputStream.reset();
            final T result;
            try (InputStream wrappedInputStream = compressed ? CompressorFactory.COMPRESSOR.threadLocalInputStream(inputStream) : inputStream) {
                final CheckedInputStream checkedInputStream = new CheckedInputStream(wrappedInputStream, new CRC32());
                try (XContentParser parser = XContentType.SMILE.xContent().createParser(
                        namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, Streams.noCloseStream(checkedInputStream))) {
                    result = reader.apply(parser);
                }
                final long checksum = checkedInputStream.getChecksum().getValue();
                final DataInput footerInput = new InputStreamDataInput(inputStream);
                if (footerInput.readInt() != CodecUtil.FOOTER_MAGIC) {
                    throw new CorruptStateException("invalid footer magic");
                }
                if (footerInput.readInt() != 0) {
                    throw new CorruptStateException("invalid codec version");
                }
                final long checksumRead = footerInput.readLong();
                if (checksumRead != checksum) {
                    throw new CorruptStateException("checksum does not match");
                }
            }
            return result;
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we trick this into a dedicated exception with the original stacktrace
            throw new CorruptStateException(ex);
        }
    }

    private static final class FooterAwareInputStream extends FilterInputStream {

        private final CRC32 crc32 = new CRC32();

        private final byte[] footerBytes = new byte[16];

        int footerOffset = 0;

        FooterAwareInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int read = super.read();
            if (footerOffset < footerBytes.length) {
                footerBytes[footerOffset++] = (byte) read;
            } else {
                System.arraycopy(footerBytes, 1, footerBytes, 0, 15);
                footerBytes[15] = (byte) read;
            }
            return read;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return super.read(b, off, len);
        }
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will optionally by compressed.
     *
     * @param obj                 object to be serialized
     * @param blobContainer       blob container
     * @param name                blob name
     * @param compress            whether to use compression
     */
    public void write(T obj, BlobContainer blobContainer, String name, boolean compress, BigArrays bigArrays) throws IOException {
        final String blobName = blobName(name);
        serialize(obj, blobName, compress, bigArrays, bytes -> blobContainer.writeBlob(blobName, bytes, false));
    }

    public void serialize(final T obj, final String blobName, final boolean compress, BigArrays bigArrays,
                          CheckedConsumer<BytesReference, IOException> consumer) throws IOException {
        try (ReleasableBytesStreamOutput outputStream = new ReleasableBytesStreamOutput(bigArrays)) {
            try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "ChecksumBlobStoreFormat.writeBlob(blob=\"" + blobName + "\")", blobName,
                    org.elasticsearch.common.io.Streams.noCloseStream(outputStream), BUFFER_SIZE)) {
                CodecUtil.writeHeader(indexOutput, codec, VERSION);
                try (OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput) {
                    @Override
                    public void close() {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    }
                }; XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE,
                        compress ? CompressorFactory.COMPRESSOR.threadLocalOutputStream(indexOutputOutputStream)
                                : indexOutputOutputStream)) {
                    builder.startObject();
                    obj.toXContent(builder, SNAPSHOT_ONLY_FORMAT_PARAMS);
                    builder.endObject();
                }
                CodecUtil.writeFooter(indexOutput);
            }
            consumer.accept(outputStream.bytes());
        }
    }
}

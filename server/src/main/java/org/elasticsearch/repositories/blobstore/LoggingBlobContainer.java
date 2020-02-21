package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

class LoggingBlobContainer implements BlobContainer {

    private final Logger logger = LogManager.getLogger(LoggingBlobContainer.class);

    private final String repository;

    private final BlobContainer delegate;

    LoggingBlobContainer(String repository, BlobContainer delegate) {
        this.repository = repository;
        this.delegate = delegate;
    }

    @Override
    public BlobPath path() {
        return delegate.path();
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return delegate.readBlob(blobName);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        delegate.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        return delegate.delete();
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        delegate.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return delegate.listBlobs();
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return delegate.children();
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        return delegate.listBlobsByPrefix(blobNamePrefix);
    }
}

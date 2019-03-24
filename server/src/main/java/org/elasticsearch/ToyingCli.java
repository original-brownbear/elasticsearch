package org.elasticsearch;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.common.UUIDs;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;

public class ToyingCli {

    public static void main(String[] args) throws IOException {
        final IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        final Path indexPath = Paths.get("/tmp", UUIDs.randomBase64UUID());
        try (Directory directory = new MMapDirectory(indexPath);
             IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
            final Collection<IndexableField> fields = Arrays.asList(
                new StringField("foo", "bar", Field.Store.YES)
            );
            indexWriter.addDocument(fields);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class BlobstoreMetaStore implements Closeable {

    private final Connection connection;

    public BlobstoreMetaStore() throws SQLException {
        final String dbName = "db";
        connection = DriverManager.getConnection("jdbc:derby:memory:myDB;create=true");
        try (PreparedStatement create = connection.prepareStatement("""
                        CREATE TABLE snapshot_infos (
                            uuid varchar (1024),
                            name varchar (1024),
                            repository varchar (1024),
                            state varchar (1024),
                            CONSTRAINT PK_SNAPSHOT PRIMARY KEY (uuid, repository)
                        )
            """)) {
            create.execute();
        }
        try (PreparedStatement create = connection.prepareStatement("""
                        CREATE TABLE snapshot_indices (
                            uuid varchar(1024),
                            repository varchar(1024),
                            name varchar (1024),
                            CONSTRAINT UC_NAME_ID UNIQUE (repository, uuid, name),
                            FOREIGN KEY (uuid, repository) REFERENCES snapshot_infos(uuid, repository)
                        )
            """)) {
            create.execute();
        }
    }

    @Nullable
    public SnapshotInfo get(String repository, String uuid) {
        try (
            PreparedStatement statement = connection.prepareStatement(
                "SELECT name, state FROM snapshot_infos WHERE repository=? AND uuid=?"
            )
        ) {
            statement.setString(1, repository);
            statement.setString(2, uuid);
            if (statement.execute() == false) {
                return null;
            }
            final ResultSet resultSet = statement.getResultSet();
            resultSet.next();
            final String name = resultSet.getString(1);
            final SnapshotState state = SnapshotState.valueOf(resultSet.getString(2));
            final List<String> indices = new ArrayList<>();
            try (
                    PreparedStatement indicesStatement = connection.prepareStatement(
                            "SELECT name FROM snapshot_indices WHERE repository=? AND uuid=?"
                    )
            ) {
                indicesStatement.setString(1, repository);
                indicesStatement.setString(2, uuid);
                if (indicesStatement.execute()) {
                    final ResultSet res = indicesStatement.getResultSet();
                    while (res.next()) {
                        indices.add(res.getString(1));
                    }
                }
            }
            return new SnapshotInfo(
                new Snapshot(repository, new SnapshotId(name, uuid)),
                indices,
                List.of(),
                List.of(),
                state
            );
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    public void put(SnapshotInfo snapshotInfo) {
        try (
            PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO snapshot_infos (uuid, name, repository, state) VALUES (?, ?, ?, ?)"
            )
        ) {
            statement.setString(1, snapshotInfo.snapshotId().getUUID());
            statement.setString(2, snapshotInfo.snapshotId().getName());
            statement.setString(3, snapshotInfo.repository());
            statement.setString(4, snapshotInfo.state().name());
            statement.execute();
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
        try (
            PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO snapshot_indices (uuid, repository,  name) VALUES (?, ?, ?)"
            )
        ) {
            statement.setString(1, snapshotInfo.snapshotId().getUUID());
            statement.setString(2, snapshotInfo.repository());
            for (String index : snapshotInfo.indices()) {
                statement.setString(3, index);
                statement.execute();
            }

        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    public void persist(String path) {
        try (PreparedStatement statement = connection.prepareStatement("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)")) {
            statement.setString(1, path);
            statement.execute();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
            try {
                DriverManager.getConnection("jdbc:derby:;shutdown=true");
            } catch (SQLException ignored) {

            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}

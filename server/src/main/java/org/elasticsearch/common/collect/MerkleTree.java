/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.collect;

import java.util.List;
import java.util.Map;

public final class MerkleTree {

    private final Map<String, Object> values;

    private MerkleTree() {

    }

    public String get(String key) {

    }

    public List<String> getList(String key) {

    }

    public static final class Builder {

        public MerkleTree build() {

        }
    }

    private static final class Node {

        private final String hash;



        private final Node left;

        private final Node right;

        Node (String hash, Node left, Node right) {

        }

    }
}

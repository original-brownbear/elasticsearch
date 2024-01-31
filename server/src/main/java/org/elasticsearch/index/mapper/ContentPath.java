/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;

public final class ContentPath {

    private static final char DELIMITER = '.';

    private StringBuilder sb;

    private int index = 0;

    private String[] path = Strings.EMPTY_ARRAY;

    private boolean withinLeafObject = false;

    public ContentPath() {}

    String[] getPath() {
        // used for testing
        return path;
    }

    public void add(String name) {
        if (index == path.length) { // expand if needed
            expand();
        }
        path[index++] = name;
    }

    private void expand() {
        String[] newPath = new String[path.length + 10];
        System.arraycopy(path, 0, newPath, 0, path.length);
        path = newPath;
    }

    public void remove() {
        path[--index] = null;
    }

    public void setWithinLeafObject(boolean withinLeafObject) {
        this.withinLeafObject = withinLeafObject;
    }

    public boolean isWithinLeafObject() {
        return withinLeafObject;
    }

    public String pathAsText(String name) {
        if (index == 0) {
            return name;
        }
        if (index == 1 && name.isEmpty()) {
            return path[0];
        }
        if (sb == null) {
            sb = new StringBuilder();
        } else {
            sb.setLength(0);
        }
        for (int i = 0; i < index; i++) {
            sb.append(path[i]).append(DELIMITER);
        }
        sb.append(name);
        return sb.toString();
    }

    public int length() {
        return index;
    }
}

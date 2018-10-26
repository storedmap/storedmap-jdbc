/*
 * Copyright 2018 Fyodor Kravchenko <fedd@vsetec.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.vsetec.storedmap.jdbc;

import java.util.Collections;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class GenericJdbcDriver extends AbstractJdbcDriver {

    @Override
    public int getMaximumIndexNameLength() {
        return 200;
    }

    @Override
    public int getMaximumKeyLength() {
        return 200;
    }

    @Override
    public int getMaximumTagLength() {
        return 200;
    }

    @Override
    public int getMaximumSorterLength() {
        return 200;
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery, String[] anyOfTags) {
        return get(indexName, connection, anyOfTags);
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        return get(indexName, connection, minSorter, maxSorter, anyOfTags, ascending);
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        return get(indexName, connection, minSorter, maxSorter, ascending);
    }

}

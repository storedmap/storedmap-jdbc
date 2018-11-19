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
import org.apache.commons.dbcp.BasicDataSource;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class GenericJdbcDriver extends AbstractJdbcDriver {

    @Override
    public int getMaximumIndexNameLength(BasicDataSource ds) {
        return 60;
    }

    @Override
    public int getMaximumKeyLength(BasicDataSource ds) {
        return 200;
    }

    @Override
    public int getMaximumTagLength(BasicDataSource ds) {
        return 200;
    }

    @Override
    public int getMaximumSorterLength(BasicDataSource ds) {
        return 200;
    }

    @Override
    public Iterable<String> get(String indexName, BasicDataSource ds, String textQuery) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Iterable<String> get(String indexName, BasicDataSource ds, String textQuery, String[] anyOfTags) {
        return get(indexName, ds, anyOfTags);
    }

    @Override
    public Iterable<String> get(String indexName, BasicDataSource ds, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        return get(indexName, ds, minSorter, maxSorter, anyOfTags, ascending);
    }

    @Override
    public Iterable<String> get(String indexName, BasicDataSource ds, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        return get(indexName, ds, minSorter, maxSorter, ascending);
    }

    @Override
    public Iterable<String> get(String indexName, BasicDataSource connection, String textQuery, int from, int size) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Iterable<String> get(String indexName, BasicDataSource connection, String textQuery, String[] anyOfTags, int from, int size) {
        return get(indexName, connection, anyOfTags, from, size);
    }

    @Override
    public Iterable<String> get(String indexName, BasicDataSource connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending, int from, int size) {
        return get(indexName, connection, minSorter, maxSorter, anyOfTags, ascending, from, size);
    }

    @Override
    public Iterable<String> get(String indexName, BasicDataSource connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending, int from, int size) {
        return get(indexName, connection, minSorter, maxSorter, ascending, from, size);
    }

}

/*
 * Copyright 2018 Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}.
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
package org.storedmap.jdbc;

import org.storedmap.Driver;
import java.util.Collections;
import org.apache.commons.dbcp.BasicDataSource;

/**
 * A generic implementation of a JDBC StoredMap {@link Driver} that totally
 * ignores the full text query
 *
 * @author Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}
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
        return 60;
    }

    @Override
    public long countWithQuery(String indexName, BasicDataSource connection, String secondaryKey, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, String textQuery) {
        return 0;
    }

    @Override
    public Iterable<String> getWithQuery(String indexName, BasicDataSource connection, String secondaryKey, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, Boolean ascending, String textQuery) {
        return Collections.EMPTY_LIST;
    }


}

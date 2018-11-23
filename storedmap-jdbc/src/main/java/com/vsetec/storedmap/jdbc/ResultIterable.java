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

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class ResultIterable implements Iterable<String>, Closeable {

    private final PreparedStatement _ps;
    private final ResultSet _rs;
    private final Connection _conn;
    private final int _stopAt;

    public ResultIterable(Connection conn, ResultSet rs, PreparedStatement ps) {
        _conn = conn;
        _rs = rs;
        _ps = ps;
        _stopAt = Integer.MAX_VALUE;
    }

    public ResultIterable(Connection conn, ResultSet rs, PreparedStatement ps, int skip, int size) {
        _conn = conn;
        _rs = rs;
        _ps = ps;
        _stopAt = size;
        try {
            for (int i = 0; i < skip; i++) {
                _rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            this.close();
        } finally {
            super.finalize();
        }
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {

            private int _i = 0;
            private boolean _endReached = false;
            private boolean _nextLoaded = false;
            private String _next = null;

            @Override
            public boolean hasNext() {
                if (_nextLoaded) {
                    return !_endReached;
                } else {

                    if (_endReached) {
                        return false;
                    }

                    _nextLoaded = true;
                    try {
                        _endReached = _i >= _stopAt || !_rs.next();
                        _i++;
                        if (_endReached) {
                            _next = null;
                            ResultIterable.this.close();
                            return false;
                        } else {
                            _next = _rs.getString(1);
                            return true;
                        }
                    } catch (SQLException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public String next() {
                if (hasNext()) {
                    String ret = _next;
                    _next = null;
                    _nextLoaded = false;
                    return ret;
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    @Override
    public void close() throws IOException {
        try {
            if (!_rs.isClosed()) {
                _conn.commit();
                _rs.close();
                _ps.close();
                _conn.close();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

}

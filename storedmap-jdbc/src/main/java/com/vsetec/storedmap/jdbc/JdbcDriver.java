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

import com.vsetec.storedmap.Driver;
import com.vsetec.storedmap.StoredMapException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.dbcp.BasicDataSource;
import org.mvel2.templates.TemplateRuntime;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class JdbcDriver implements Driver {
    
    private final Map<String, Integer> _tablesAndSorterFieldNumber = new HashMap<>();

    @Override
    public Object openConnection(String connectionString, Properties properties) {
        
        BasicDataSource ds = new BasicDataSource();
        
        ds.setUrl(properties.getProperty(connectionString));
        ds.setDriverClassName(properties.getProperty("storedmap.jdbc.driver"));
        ds.setUsername(properties.getProperty("storedmap.jdbc.user"));
        ds.setPassword(properties.getProperty("storedmap.jdbc.password"));
        properties.entrySet().forEach((entry) -> {
            ds.addConnectionProperty((String)entry.getKey(), (String)entry.getValue());
        });
        
        return ds;
    }

    @Override
    public void closeConnection(Object connection) {
        try{
            ((BasicDataSource)connection).close();
        }catch(SQLException e){
            throw new StoredMapException("Couldn'c close the connection", e);
        }
    }

    
    private synchronized void _createMainTable(Connection conn, String table) throws SQLException{
        if(_tablesAndSorterFieldNumber.containsKey(table)){
            return;
        }
        
        try{
            Statement s = conn.createStatement();
            s.execute("select id from " + table + " where id is null"); // dumb test for table existence
        }catch(SQLException e){
            Map<String,String>vars = new HashMap<>(3);
            vars.put("indexName", table);
            String sql = (String) TemplateRuntime.eval(this.getClass().getResourceAsStream("main.sql"), vars);

            Statement st = conn.createStatement();
            st.executeUpdate(sql);
        }
    }
    
    
    @Override
    public byte[] get(String key, String indexName, Object connection) {
        BasicDataSource ds = (BasicDataSource)connection;
        
        try{ // TODO: convert all to try with resources
            Connection conn = ds.getConnection();
            _createMainTable(conn, indexName);


            PreparedStatement ps = conn.prepareStatement("select value from " + indexName + " where id=?");
            ps.setString(0, key);
            ResultSet rs = ps.executeQuery();
            
            byte[]ret;
            if(rs.next()){
                ret = rs.getBytes(0);
            }else{
                ret = null;
            }
            
            rs.close();
            ps.close();
            conn.close();
            
            return ret;

        }catch(SQLException e){
            throw new RuntimeException(e);
        }
        
    }

    @Override
    public Iterable<String> get(String indexName, Object connection) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String[] anyOfTags) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, byte[] minSorter, byte[] maxSorter) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery, String[] anyOfTags) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long tryLock(String key, String indexName, Object connection, long milliseconds) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void unlock(String key, String indexName, Object connection) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void put(String key, String indexName, Object connection, byte[] value, Runnable callbackOnIndex, Map<String, Object> map, List<Locale> locales, List<Byte> sorter, List<String> tags, Runnable callbackOnAdditionalIndex) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String[] getTags(String key, String indexName, Object connection) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public byte[] getSorter(String key, String indexName, Object connection) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeTagsSorterAndFulltext(String key, String indexName, Object connection, Runnable callback) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

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

}

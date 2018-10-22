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
import java.sql.Timestamp;
import java.time.temporal.TemporalField;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.dbcp.BasicDataSource;
import org.mvel2.templates.TemplateRuntime;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class JdbcDriver implements Driver {
    
    private final Set<String> _tables = new HashSet<>();
    private final Base32 _b32 = new Base32(true);

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
            BasicDataSource ds = (BasicDataSource)connection;
            ds.close();
        }catch(SQLException e){
            throw new StoredMapException("Couldn'c close the connection", e);
        }
    }

    
    private synchronized void _createTableSet(Connection conn, String table) throws SQLException{
        if(_tables.contains(table)){
            return;
        }

        Map<String,String>vars = new HashMap<>(3);
        vars.put("indexName", table);
        String allSqls = (String) TemplateRuntime.eval(this.getClass().getResourceAsStream("create.sql"), vars);
        String[]sqls = allSqls.split(";");

        try{
            Statement s = conn.createStatement();
            s.execute("select id from " + table + " where id is null"); // dumb test for table existence
        }catch(SQLException e){
            for(String sql: sqls){
                Statement st = conn.createStatement();
                st.executeUpdate(sql);
            }
            _tables.add(table);
        }
    }


    @Override
    public void put(
            String key, 
            String indexName, 
            Object connection, 
            byte[] value, 
            Runnable callbackOnIndex, 
            Map<String, Object> map, 
            List<Locale> locales, 
            List<Byte> sorter, 
            List<String> tags, 
            Runnable callbackOnAdditionalIndex) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int tryLock(String key, String indexName, Object connection, int milliseconds) {
        BasicDataSource ds = (BasicDataSource)connection;
        try{ // TODO: convert all to try with resources
            Connection conn = ds.getConnection();
            _createTableSet(conn, indexName);

            PreparedStatement ps = conn.prepareStatement("select current_timestamp, createdat, waitfor from " + indexName + "_lock where id=?");
            ps.setString(1, key);
            ResultSet rs = ps.executeQuery();
            
            int millisStillToWait;
            
            Timestamp currentTime;
            if(rs.next()){
                currentTime = rs.getTimestamp(1);
                Timestamp createdat = rs.getTimestamp(2);
                int waitfor = rs.getInt(3);
                millisStillToWait = (int) (createdat.toInstant().toEpochMilli() + waitfor - currentTime.toInstant().toEpochMilli());
                
            }else{
                currentTime = null;
                millisStillToWait = 0;
            }
            rs.close();
            ps.close();
            
            // write lock time if we are not waiting anymore
            if(millisStillToWait<=0){
                
                if(currentTime==null){ // there was no lock record
                    ps = conn.prepareStatement("insert into " + indexName + "_lock (id, createdat, waitfor) values (?, current_timestamp, ?)");
                    ps.setString(1, key);
                    ps.setInt(2, milliseconds);
                    ps.executeUpdate();
                    ps.close();
                }else{
                    ps = conn.prepareStatement("update " + indexName + "_lock set createdat=current_timestamp, waitfor=? where id=?");
                    ps.setInt(1, milliseconds);
                    ps.setString(2, key);
                    ps.executeUpdate();
                    ps.close();
                }
                
            }
            
            conn.commit();
            conn.close();
            
            return millisStillToWait;

        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unlock(String key, String indexName, Object connection) {
        BasicDataSource ds = (BasicDataSource)connection;
        try{ // TODO: convert all to try with resources
            Connection conn = ds.getConnection();
            _createTableSet(conn, indexName);

            PreparedStatement ps = conn.prepareStatement("delete from " + indexName + "_lock where id=?");
            ps.setString(1, key);
            ps.executeUpdate();
            ps.close();
            conn.commit();
            conn.close();
        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public byte[] get(String key, String indexName, Object connection) {
        BasicDataSource ds = (BasicDataSource)connection;
        try{ // TODO: convert all to try with resources
            Connection conn = ds.getConnection();
            _createTableSet(conn, indexName);

            PreparedStatement ps = conn.prepareStatement("select val from " + indexName + " where id=?");
            ps.setString(1, key);
            ResultSet rs = ps.executeQuery();
            
            byte[]ret;
            if(rs.next()){
                ret = rs.getBytes(1);
            }else{
                ret = null;
            }
            
            rs.close();
            ps.close();
            conn.commit();
            conn.close();
            
            return ret;

        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Iterable<String> get(String indexName, Object connection) {
        BasicDataSource ds = (BasicDataSource)connection;
        try{ // TODO: convert all to try with resources
            Connection conn = ds.getConnection();
            _createTableSet(conn, indexName);

            PreparedStatement ps = conn.prepareStatement("select id from " + indexName);
            ResultSet rs = ps.executeQuery();

            ResultIterable ri = new ResultIterable(conn, rs, ps);

            return ri;
        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String[] anyOfTags) {
        BasicDataSource ds = (BasicDataSource)connection;
        try{ // TODO: convert all to try with resources
            Connection conn = ds.getConnection();
            _createTableSet(conn, indexName);

            String tagQ = "";
            for(int i=0;i<anyOfTags.length;i++){
                if(i==0){
                    tagQ = "?";
                }else{
                tagQ = tagQ + ",?";  // TODO: optimize with stringbuilder, if it seems really faster (which is not nesesserily true)
                }
            } 
            
            PreparedStatement ps = conn.prepareStatement("select distinct id from " + indexName + "_tags where tag in (" + tagQ + ")");
            for(int i=0;i<anyOfTags.length;i++){
                ps.setString(i+1, anyOfTags[i]);
            }

            ResultSet rs = ps.executeQuery();

            ResultIterable ri = new ResultIterable(conn, rs, ps);

            return ri;
        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        BasicDataSource ds = (BasicDataSource)connection;
        try{ // TODO: convert all to try with resources
            Connection conn = ds.getConnection();
            _createTableSet(conn, indexName);

            String sql = "select id from " + indexName + "_sort ";
            if(minSorter!=null){
                if(maxSorter!=null){
                    sql = sql + "where sort > ? and sort <= ?";
                }else{
                    sql = sql + "where sort > ?";
                }
            }else{
                if(maxSorter!=null){
                    sql = sql + "where sort <= ?";
                }
            }
            
            if(ascending){
                sql = sql + " order by sort";
            }else{
                sql = sql + " order by sort desc";
            }
            
            PreparedStatement ps = conn.prepareStatement(sql);
            int i=1;
            if(minSorter!=null){
                ps.setString(i, _b32.encodeAsString(minSorter));
                i++;
            }
            if(maxSorter!=null){
                ps.setString(i, _b32.encodeAsString(maxSorter));
                i++;
            }
            ResultSet rs = ps.executeQuery();

            ResultIterable ri = new ResultIterable(conn, rs, ps);

            return ri;
        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        BasicDataSource ds = (BasicDataSource)connection;
        try{ // TODO: convert all to try with resources
            Connection conn = ds.getConnection();
            _createTableSet(conn, indexName);

            String sql = "select distinct " + indexName + "_sort.id from " + indexName + "_sort inner join " + indexName + "_tags on " + indexName + "_sort.id=" + indexName + "_tags.id where ";

            String tagQ = "";
            for(int i=0;i<anyOfTags.length;i++){
                if(i==0){
                    tagQ = "?";
                }else{
                tagQ = tagQ + ",?";  // TODO: optimize with stringbuilder, if it seems really faster (which is not nesesserily true)
                }
            } 

            if(minSorter!=null){
                if(maxSorter!=null){
                    sql = sql + "sort > ? and sort <= ? and tag in ("+tagQ+")";
                }else{
                    sql = sql + "sort > ? and tag in ("+tagQ+")";
                }
            }else{
                if(maxSorter!=null){
                    sql = sql + "sort <= ? and tag in ("+tagQ+")";
                }else{
                    sql = sql + "tag in ("+tagQ+")";
                }
            }
            
            if(ascending){
                sql = sql + " order by sort";
            }else{
                sql = sql + " order by sort desc";
            }
            
            PreparedStatement ps = conn.prepareStatement(sql);
            int i=1;
            if(minSorter!=null){
                ps.setString(i, _b32.encodeAsString(minSorter));
                i++;
            }
            if(maxSorter!=null){
                ps.setString(i, _b32.encodeAsString(maxSorter));
                i++;
            }
            
            for(String tag: anyOfTags){
                ps.setString(i, tag);
                i++;
            }

            ResultSet rs = ps.executeQuery();

            ResultIterable ri = new ResultIterable(conn, rs, ps);

            return ri;
        }catch(SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery, String[] anyOfTags) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void remove(String key, String indexName, Object connection, Runnable callback) {
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

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
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.dbcp.BasicDataSource;
import org.mvel2.templates.CompiledTemplate;
import org.mvel2.templates.TemplateCompiler;
import org.mvel2.templates.TemplateRuntime;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class JdbcDriver implements Driver {
    
    private final Base32 _b32 = new Base32(true);
    private final Map<String,Map<String,Object>> _indicesAndMvelContext = new HashMap<>();
    private final Map<String,CompiledTemplate> _dynamicSql = new HashMap<>();
    private final Map<String,Map<String,String>> _indexStaticSql = new HashMap<>();

    @Override
    public Object openConnection(String connectionString, Properties properties) {
        
        Properties sqlProps = new Properties();
        try{
            sqlProps.load(this.getClass().getResourceAsStream("queries.properties"));
        }catch(IOException e){
            throw new RuntimeException("Couldn't initialize driver", e);
        }

        _dynamicSql.clear();
        _indexStaticSql.clear();
        for(String queryName : sqlProps.stringPropertyNames()){
            String sqlTemplate = sqlProps.getProperty(queryName);
            CompiledTemplate ct = TemplateCompiler.compileTemplate(sqlTemplate);
            _dynamicSql.put(queryName, ct);
            _indexStaticSql.put(queryName, new HashMap<>());
        }
        
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
        if(_indicesAndMvelContext.containsKey(table)){
            return;
        }

        Map<String,String>vars = new HashMap<>(3);
        vars.put("indexName", table);
        String allSqls = (String) TemplateRuntime.execute(_dynamicSql.get("create"), vars);
        String[]sqls = allSqls.split(";");
        String checkSql = (String) TemplateRuntime.execute(_dynamicSql.get("check"), vars);

        try{
            Statement s = conn.createStatement();
            s.execute(checkSql); // dumb test for table existence
        }catch(SQLException e){
            for(String sql: sqls){
                Statement st = conn.createStatement();
                st.executeUpdate(sql);
            }
            _indicesAndMvelContext.put(table, Collections.unmodifiableMap(vars));
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

    private String _getSql(String indexName, String queryName, Object...paramsNameValue){
        
        String ret;
        Map<String,String>stat;
        if(paramsNameValue==null||paramsNameValue.length==0){
            stat = _indexStaticSql.get(queryName);
            synchronized(stat){
                ret = _indexStaticSql.get(queryName).get(indexName);
            }
        }else{
            ret = null;
            stat = null;
        }
        
        if(ret==null){
            CompiledTemplate ct = _dynamicSql.get(queryName);
            Map<String,Object>context = _indicesAndMvelContext.get(indexName);
            
            if(stat!=null){
                ret = (String) TemplateRuntime.execute(ct, context);
                synchronized(stat){
                    stat.put(indexName, ret);
                }
            }else{
                
                context = new HashMap<>(context);
                for(int i=0;i<paramsNameValue.length;i++){
                    context.put((String) paramsNameValue[i], paramsNameValue[++i]);
                }
                ret = (String) TemplateRuntime.execute(ct, context);
                
            }
        }
        
        return ret;
        
    }
    
    @Override
    public int tryLock(String key, String indexName, Object connection, int milliseconds) {
        BasicDataSource ds = (BasicDataSource)connection;
        try{ // TODO: convert all to try with resources
            Connection conn = ds.getConnection();
            _createTableSet(conn, indexName);
            String sql = _getSql(indexName, "selectLock");
            PreparedStatement ps = conn.prepareStatement(sql);
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
                    sql = _getSql(indexName, "insertLock");
                    ps = conn.prepareStatement(sql);
                    ps.setString(1, key);
                    ps.setInt(2, milliseconds);
                    ps.executeUpdate();
                    ps.close();
                }else{
                    sql = _getSql(indexName, "updateLock");
                    ps = conn.prepareStatement(sql);
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
            String sql = _getSql(indexName, "deleteLock");
            PreparedStatement ps = conn.prepareStatement(sql);
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

            PreparedStatement ps = conn.prepareStatement(_getSql(indexName, "selectById"));
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

            PreparedStatement ps = conn.prepareStatement(_getSql(indexName, "selectAll"));
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
            PreparedStatement ps = conn.prepareStatement(_getSql(indexName, "selectById", "tags", anyOfTags));

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

            PreparedStatement ps = conn.prepareStatement(_getSql(indexName, "selectByTags", "minSorter", minSorter, "maxSorter", maxSorter, "ascending", ascending));
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

            PreparedStatement ps = conn.prepareStatement(_getSql(indexName, "selectByTags", "tags", anyOfTags, "minSorter", minSorter, "maxSorter", maxSorter, "ascending", ascending));
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

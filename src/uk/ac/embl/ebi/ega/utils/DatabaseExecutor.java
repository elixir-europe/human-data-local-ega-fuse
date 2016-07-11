/*
 * Copyright 2015 EMBL-EBI.
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
package uk.ac.embl.ebi.ega.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;

/**
 *
 * @author asenf
 */
public class DatabaseExecutor {
    // Ini file location default
    private String iniFile = "fuse.ini";
    private String email = null;
    private String password = null;
    private int AES_bits = 256;
    
    // Database Pool
    private DataSource dbSource;
    
    // Query Strings (populated from ini file)
    private String datasets = null;
    private String datasets_by_email = null;
    private String files_by_dataset = null;
    private String files_by_email = null;
    
    // Additional Info
    private String fileKey = null;
    
    public DatabaseExecutor(String iniFile, String email, String password, int AES_bits) {
        this.iniFile = iniFile;
        this.email = email;
        this.password = password.length()>0?password:null;
        this.AES_bits = AES_bits;
        
        // Read Ini File, configure database
        Ini ini = null;
        try {
            ini = new Ini(new File(this.iniFile));
        } catch (IOException ex) {
            Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Read initialization file 
        if (ini != null) {
            // Database Connection ---------------------------------------------
            Section section = ini.get("database");
            
            // Configure database pool with it
            String instance = "";
            if (section.containsKey("instance"))
                instance = section.get("instance");
            String port = "";
            if (section.containsKey("port"))
                    port = section.get("port");
            String database = "";
            if (section.containsKey("database"))
                database = section.get("database");
            String user = "";
            if (section.containsKey("username"))
                user = section.get("username");
            String pass = "";
            if (section.containsKey("password"))
                pass = section.get("password");
            
            this.dbSource = MyDataSourceFactory.getHikariDataSource(instance, port, database, user, pass);
            
            // Populate query strings with it ----------------------------------
            Section queries = ini.get("queries");
            
            if (queries.containsKey("all_datasets"))
                this.datasets = queries.get("all_datasets");
            if (queries.containsKey("all_datasets_by_email"))
                this.datasets_by_email = queries.get("all_datasets_by_email");
            if (queries.containsKey("files_by_dataset"))
                this.files_by_dataset = queries.get("files_by_dataset");
            if (queries.containsKey("files_by_email"))
                this.files_by_email = queries.get("files_by_email");

            // Get file Key (demo version) -------------------------------------
            Section keys = ini.get("key");
            
            if (keys.containsKey("file_key"))
                this.fileKey = keys.get("file_key");
        }
    }
    
    // -------------------------------------------------------------------------
    // --- Getters and Setters -------------------------------------------------
    // -------------------------------------------------------------------------

    // TODO
    
    // -------------------------------------------------------------------------
    // --- Database Execution Functions ----------------------------------------
    // -------------------------------------------------------------------------
    
    
    public String[] getDatasets() {
        String[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<String> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.datasets);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results
                while (rs.next()) {
                    String set = rs.getString(1);
                    resultset.add(set);
                }
                
                // Place result in Array
                result = resultset.toArray(new String[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public String[] getDatasetsByEmail(String email) {
        String[] result = null;

        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<String> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.datasets_by_email);
                ps.setString(1, email);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results
                while (rs.next()) {
                    String set = rs.getString(1);
                    resultset.add(set);
                }
                
                // Place result in Array
                result = resultset.toArray(new String[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
        
    public EgaArchiveFile[] getFilesByDataset(String dataset) {
        EgaArchiveFile[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<EgaArchiveFile> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.files_by_dataset);
                ps.setString(1, dataset);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results: stable_id, file_name, index_name, size
                while (rs.next()) {
                    String fileName = rs.getString(1);
                    String fileArchivePath = rs.getString(2);
                    
                    resultset.add(new EgaArchiveFile(fileName, fileArchivePath, this.password, this.AES_bits));
                }
                                
                // Place result in Array
                result = resultset.toArray(new EgaArchiveFile[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    public EgaArchiveFile[] getFilesByEmail(String email) {
        EgaArchiveFile[] result = null;
        
        Connection conn = createConnection();
        if (conn != null) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ArrayList<EgaArchiveFile> resultset = new ArrayList<>();
                
                ps = conn.prepareStatement(this.files_by_email);
                ps.setString(1, email);

                // Execute query
                rs = ps.executeQuery();
                
                // Loop over results: stable_id, file_name, index_name, size
                while (rs.next()) {
                    String fileName = rs.getString(1);
                    String fileArchivePath = rs.getString(2);

                    resultset.add(new EgaArchiveFile(fileName, fileArchivePath, this.password, this.AES_bits));
                }
                
                // Place result in Array
                result = resultset.toArray(new EgaArchiveFile[resultset.size()]);
            } catch (SQLException ex) {
                Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeQuitely(rs);
                closeQuitely(ps);
            }
        }
        
        return result;
    }
    
    // -------------------------------------------------------------------------
    // --- DB Access Functions -------------------------------------------------
    // -------------------------------------------------------------------------

    private Connection createConnection() {
        Connection connection = null;
        try {
            connection = this.dbSource.getConnection();
            connection.setAutoCommit(true);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }

        return connection;
    }    
    
    private Connection createConnection(DataSource dataSource) throws SQLException {
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(true);

        return connection;
    }    
    
    // -------------------------------------------------------------------------
    // --- Close connections without considering error messages ----------------
    // -------------------------------------------------------------------------

    private void closeQuitely(Statement stmt) {
        if(stmt != null) {
	    Connection con = null;
	    try {
		con = stmt.getConnection();
	    } catch (SQLException e) {
	    }
	    try {
                stmt.close();
            } catch (SQLException e) {
                // ignore
            }
	    closeQuitely(con);
        }
    }

    private void closeQuitely(ResultSet rs) {
        if(rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private void closeQuitely(Connection con) {
	if (con != null) {
	    try {
		con.close();
	    } catch (SQLException e) {
		// ignore
	    }
	}
    }

    
    // -------------------------------------------------------------------------
    // --- Additional Functionality --------------------------------------------
    // -------------------------------------------------------------------------
    
    // Query the Fire Metadata Server for actual File Path/URL, given a relative path
    private String[] getPath(String path) {
        if (path.equalsIgnoreCase("Virtual File")) return new String[]{"Virtual File"};
        
        // EBI Internal - Not Implemented
        /*
        try {
            ArrayList<String> temp_path = new ArrayList<>();
            String[] result = new String[4]; // [0] name [1] stable_id [2] size [3] rel path
            result[0] = "";
            result[1] = "";
            result[3] = path;
            String path_ = path;

            // Sending Request
            HttpURLConnection connection = null;
            connection = (HttpURLConnection)(new URL("..")).openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("..", "..");
            connection.setRequestProperty("..", "..");
            connection.setRequestProperty("..", "..");

            // Reading Response
            int responseCode = connection.getResponseCode();

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            ArrayList<String[]> paths = new ArrayList<>();
            
            String location_http = "", 
                   location_http_tag = "", 
                   location_md5 = "";
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.startsWith("FILE_PATH"))
                    temp_path.add(inputLine.substring(inputLine.indexOf("/")).trim());
                if (inputLine.startsWith("HTTP_GET"))
                    location_http = inputLine.substring(inputLine.indexOf("http://")).trim();
                if (inputLine.startsWith("AUTH_BASIC"))
                    location_http_tag = inputLine.substring(inputLine.indexOf(" ")+1).trim();
                if (inputLine.startsWith("FILE_MD5")) {
                    location_md5 = inputLine.substring(inputLine.indexOf(" ")+1).trim();
                    paths.add(new String[]{location_http, location_http_tag, location_md5});
                }
            }
            in.close();

            if (paths.size() > 0) {
                for (int i=0; i<paths.size(); i++) {
                    String[] e = paths.get(i);
                    if (e[1].contains("egaread")) {
                        result[0] = e[0];
                        result[1] = e[1];
                        result[2] = String.valueOf(getLength(new String[]{location_http, location_http_tag}));
                    }
                }
            } else if (temp_path.size()>0 && result[1].length()==0) { // Determine proper path
                for (String temp_path1 : temp_path) {
                    if ((new File(temp_path1)).exists()) {
                        result[0] = temp_path1;
                        result[2] = String.valueOf((new File(temp_path1)).length());
                        break;
                    }
                }
                result[1] = "";
            }
            
            return result;
        } catch (Exception e) {
            System.out.println("Path = " + path);
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }            
        */
        
        return null;
    }

    // Get the length of a file, from disk or Cleversafe server
    private long getLength(String[] path) {
        long result = -1;
        
        try {
            if (path[1] != null && path[1].length() == 0) { // Get size of file directly
                File f = new File(path[0]);
                result = f.length();
            } else { // Get file size from HTTP
                // Sending Request
                HttpURLConnection connection = null;
                connection = (HttpURLConnection)(new URL(path[0])).openConnection();
                connection.setRequestMethod("HEAD");

                String userpass = path[1];
                
                // Java bug : http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6459815
                String encoding = new sun.misc.BASE64Encoder().encode (userpass.getBytes());
                encoding = encoding.replaceAll("\n", "");  
                
                String basicAuth = "Basic " + encoding;
                connection.setRequestProperty ("Authorization", basicAuth);
                
                // Reading Response
                int responseCode = connection.getResponseCode();

                String headerField = connection.getHeaderField("content-length");
                String temp = headerField.trim();
                result = Long.parseLong(temp);

                connection.disconnect();
            }
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace();
        }            
        
        return result;
    }
    
}

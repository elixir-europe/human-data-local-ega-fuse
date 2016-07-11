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

package uk.ac.embl.ebi.ega.egafuselayer;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import uk.ac.embl.ebi.ega.filesystems.EgaMemoryCIPFuse;
import uk.ac.embl.ebi.ega.utils.DatabaseExecutor;
import uk.ac.embl.ebi.ega.utils.EgaArchiveFile;

/**
 *
 * @author asenf
 */
public class EgaFUSELayer {
    /**
     * @param args the command line arguments
     * 
     * Mount Directory: must be empty, will hold FUSE files, apparently unencrypted
     * Password: CIP File password - must be the same for all files in the source
     * AES Key Bit length: 128-bit or 256-bit
     */
    public static void main(String[] args) {
        // ---------------------------------------------------------------------        
        // No parameters - exit
        if (args.length == 0)
            return;

        // Command line parameters for FUSE
        Options options = new Options();

        options.addOption("m", true, "mountpoint"); // Where virtual files are mounted        
        options.addOption("d", true, "dataset"); // Allows to mount one specified dataset 
        options.addOption("u", true, "user"); // Select files for one specified user        
        options.addOption("l", true, "path"); // Path for INI file
        options.addOption("i", true, "ini"); // Name for INI file
        options.addOption("k", true, "bits"); // Encryption Bits
        options.addOption("p", true, "password"); // Archive Password
        options.addOption("t", false, "test"); // Test Option

        // Option Parameters Default Values
        String mountpoint = "";
        String dataset = "*";
        String user = "*";
        String path = "";
        String ini = "fuse.ini";
        String bits = "256";
        String password = null;
        boolean test = false;
        
        // Parse Parameters
        CommandLineParser parser = new BasicParser();
        try {        
            CommandLine cmd = parser.parse( options, args);
            
            if (cmd.hasOption("m"))
                mountpoint = cmd.getOptionValue("m");
            if (cmd.hasOption("d"))
                dataset = cmd.getOptionValue("d");
            if (cmd.hasOption("u"))
                user = cmd.getOptionValue("u");
            if (cmd.hasOption("l"))
                path = cmd.getOptionValue("l");
            if (cmd.hasOption("i"))
                ini = cmd.getOptionValue("i");
            if (cmd.hasOption("k"))
                bits = cmd.getOptionValue("k");
            if (cmd.hasOption("p"))
                password = cmd.getOptionValue("p");
            if (cmd.hasOption("t"))
                test = true;
            
        } catch (ParseException ex) {
            System.out.println("Unrecognized Parameter. Use '-m'  '-d'  '-u'  '-l'  '-i'  '-k'  '-p'.");
            Logger.getLogger(EgaFUSELayer.class.getName()).log(Level.SEVERE, null, ex);
        }

        // ---------------------------------------------------------------------
        // Database: get files to be included; also specify a key and encryption bits, if specified
        int AES_bits = Integer.parseInt(bits);
        EgaArchiveFile[] files = getFiles(path, ini, user, password, AES_bits);
        if (test) { // Just test database
            System.out.println("files: " + (files==null));
            for (EgaArchiveFile f: files)
                System.out.println("  " + f.getFileArchivePath() + "; " + f.getFileName());
            return;
        } 
        
        // ---------------------------------------------------------------------
        // Start the FUSE file system - instatiate each individual file
        EgaMemoryCIPFuse fs = new EgaMemoryCIPFuse(files, mountpoint);
        fs.run();
    }

    // Connect to DB; Query for files; return list
    private static EgaArchiveFile[] getFiles(String iniPath, String iniName, String email, String password, int AES_bits) {
        String path = iniPath; // Build path to ini file from parameters
        if (path.length() > 0 && !path.endsWith("/")) path += "/";
        path += iniName;
        DatabaseExecutor dbe = new DatabaseExecutor(path, email, password, AES_bits);
        
        // Obtain files (potentially limited by user and/or dataset)
        EgaArchiveFile[] result = dbe.getFilesByEmail(email);        
        return result;
    }
    
}

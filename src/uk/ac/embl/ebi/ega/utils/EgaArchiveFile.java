/*
 * Copyright 2016 EMBL-EBI.
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

/**
 *
 * @author asenf
 */
public class EgaArchiveFile {

    private String fileName = null;
    private String fileArchivePath = null;
    private String fileKey = null;
    private int fileAESBits = 128;
    private boolean isEncrypted = false;

    public EgaArchiveFile(String fileName, String fileArchivePath, String fileKey, int fileAESBits) {
        this.fileName = fileName;
        this.fileArchivePath = fileArchivePath;
        this.fileKey = fileKey;
        this.fileAESBits = fileAESBits;
        this.isEncrypted = (this.fileKey!=null);
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setFileArchivePath(String fileArchivePath) {
        this.fileArchivePath = fileArchivePath;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }
    
    public void setFileAESKeyBits(int fileAESKeyBits) {
        this.fileAESBits = fileAESKeyBits;
    }

    public String getFileName() {
        return this.fileName;
    }
    
    public String getFileArchivePath() {
        return this.fileArchivePath;
    }
    
    public String getFileKey() {
        return this.fileKey;
    }
    
    public int getFileAESKeyBits() {
        return this.fileAESBits;
    }
    
    public boolean isEncrypted() {
        return this.isEncrypted;
    }
}

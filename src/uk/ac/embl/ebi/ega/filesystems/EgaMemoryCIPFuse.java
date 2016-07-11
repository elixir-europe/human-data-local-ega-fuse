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
package uk.ac.embl.ebi.ega.filesystems;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.samtools.seekablestream.SeekableFileStream;
import net.sf.samtools.seekablestream.SeekableStream;
import uk.ac.embl.ebi.ega.utils.EgaArchiveFile;
import uk.ac.embl.ebi.ega.utils.SeekableCipherStream_256;

public class EgaMemoryCIPFuse extends FuseStubFS {
    
    // ************************************************************************* MemoryDirectory
    
    private class MemoryDirectory extends MemoryPath {
        private List<MemoryPath> contents = new ArrayList<>();

        private MemoryDirectory(String name) {
            super(name);
        }

        private MemoryDirectory(String name, MemoryDirectory parent) {
            super(name, parent);
        }

        public synchronized void add(MemoryPath p) {
            contents.add(p);
            p.parent = this;
        }

        private synchronized void deleteChild(MemoryPath child) {
            contents.remove(child);
        }

        @Override
        protected MemoryPath find(String path) {
            if (super.find(path) != null) {
                return super.find(path);
            }
            while (path.startsWith("/")) {
                path = path.substring(1);
            }
            synchronized (this) {
                if (!path.contains("/")) {
                    for (MemoryPath p : contents) {
                        if (p.name.equals(path)) {
                            return p;
                        }
                    }
                    return null;
                }
                String nextName = path.substring(0, path.indexOf("/"));
                String rest = path.substring(path.indexOf("/"));
                for (MemoryPath p : contents) {
                    if (p.name.equals(nextName)) {
                        return p.find(rest);
                    }
                }
            }
            return null;
        }

        @Override
        protected void getattr(FileStat stat) {
            stat.st_mode.set(FileStat.S_IFDIR);
        }

        private synchronized void mkdir(String lastComponent) {
            contents.add(new MemoryDirectory(lastComponent, this));
        }

        /*
        public synchronized void mkfile(String lastComponent) {
            contents.add(new MemoryFile(lastComponent, this));
        }
        */

        public synchronized void read(Pointer buf, FuseFillDir filler) {
            for (MemoryPath p : contents) {
                filler.apply(buf, p.name, null, 0);
            }
        }
    }
    
    //************************************************************************** MemoryFile
    // TODO: Read file, decrypt
    
    private class MemoryFile extends MemoryPath {
        private File the_file;
        private SeekableStream the_stream;
        private String the_password;
        private boolean in_encrypted;
        private int bits;
        
        public MemoryFile(File f, String pw) {
            super(f.getName());
            this.the_file = f;
            String name = f.getName();
            this.in_encrypted = name.toLowerCase().endsWith(".cip");
            this.the_password = pw;
            this.the_stream = null;
            this.bits = 128;
        }
        public MemoryFile(File f, String pw, int bits) {
            super(f.getName());
            this.the_file = f;
            String name = f.getName();
            this.in_encrypted = name.toLowerCase().endsWith(".cip");
            this.the_password = pw;
            this.the_stream = null;
            this.bits = bits;
        }
        public MemoryFile(EgaArchiveFile f) {
            super((new File(f.getFileArchivePath())).getName());
            this.the_file = new File(f.getFileArchivePath());
            this.in_encrypted = f.isEncrypted();
            this.the_password = f.getFileKey();
            this.the_stream = null;
            this.bits = f.getFileAESKeyBits();
        }

        @Override
        protected void getattr(FileStat stat) {
            stat.st_mode.set(FileStat.S_IFREG | 0777);
            long size = this.the_file.length();
            if (this.in_encrypted)
                size = size - 16;
            stat.st_size.set(size);
        }

        // Deal with encrypted as well as unencrypted files
        public int open() {
            try {
                if (this.in_encrypted)
                    this.the_stream = new SeekableCipherStream_256(new SeekableFileStream(this.the_file), this.the_password.toCharArray(), 65535, this.bits);
                else
                    this.the_stream = new SeekableFileStream(this.the_file);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(EgaMemoryCIPFuse.class.getName()).log(Level.SEVERE, null, ex);
            }

            return 0;
        }

        private int read(Pointer buffer, long size, long offset) {
            // Get the size of the file
            long fsize = this.the_file.length();
            if (this.in_encrypted)
                fsize = fsize - 16;
            int bytesToRead = (int) Math.min(fsize - offset, size);
            // Prepare buffer to read from file
            byte[] bytesRead = new byte[bytesToRead];
            synchronized (this) {
                try {
                    this.the_stream.seek(offset);
                    this.the_stream.read(bytesRead, 0, bytesToRead);
                    buffer.put(0, bytesRead, 0, bytesToRead); // Set read data to return buffer
                    this.the_stream.seek(0);
                } catch (IOException ex) {
                    Logger.getLogger(EgaMemoryCIPFuse.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return bytesToRead;
        }

        /*
        private synchronized void truncate(long size) {
            if (size < contents.capacity()) {
                // Need to create a new, smaller buffer
                ByteBuffer newContents = ByteBuffer.allocate((int) size);
                byte[] bytesRead = new byte[(int) size];
                contents.get(bytesRead);
                newContents.put(bytesRead);
                contents = newContents;
            }
        }
        */

        /*
        private int write(Pointer buffer, long bufSize, long writeOffset) {
            int maxWriteIndex = (int) (writeOffset + bufSize);
            byte[] bytesToWrite = new byte[(int) bufSize];
            synchronized (this) {
                if (maxWriteIndex > contents.capacity()) {
                    // Need to create a new, larger buffer
                    ByteBuffer newContents = ByteBuffer.allocate(maxWriteIndex);
                    newContents.put(contents);
                    contents = newContents;
                }
                buffer.get(0, bytesToWrite, 0, (int) bufSize);
                contents.position((int) writeOffset);
                contents.put(bytesToWrite);
                contents.position(0); // Rewind
            }
            return (int) bufSize;
        }
        */
    }

    // ************************************************************************* MemoryPath
    
    private abstract class MemoryPath {
        private String name;
        private MemoryDirectory parent;

        private MemoryPath(String name) {
            this(name, null);
        }

        private MemoryPath(String name, MemoryDirectory parent) {
            this.name = name.toLowerCase().endsWith(".cip")?name.substring(0, name.length()-4):name;
            this.parent = parent;
        }

        private synchronized void delete() {
            if (parent != null) {
                parent.deleteChild(this);
                parent = null;
            }
        }

        protected MemoryPath find(String path) {
            while (path.startsWith("/")) {
                path = path.substring(1);
            }
            if (path.equals(name) || path.isEmpty()) {
                return this;
            }
            return null;
        }

        protected abstract void getattr(FileStat stat);

        private void rename(String newName) {
            while (newName.startsWith("/")) {
                newName = newName.substring(1);
            }
            name = newName;
        }
    }

    // *************************************************************************
    // *************************************************************************
    // *************************************************************************
    // Main Program
    
    private final String mount_path;
    private final EgaArchiveFile[] files;
    
    public void run() {
        try {
            String[] args = new String[]{"-o", "allow_other"}; // Allow non-root access
            this.mount(Paths.get(this.mount_path), true, true, args);
        } finally {
            this.umount();
        }
    }

    private MemoryDirectory rootDirectory = new MemoryDirectory("");

    // Instantiate a filesystem: files anf paths are provided in a list
    // File list can be obtained from a directory or a database
    public EgaMemoryCIPFuse(EgaArchiveFile[] files, String mount_path) {
        this.files = files;
        this.mount_path = mount_path;
    
        // Build File System by parsing specified origin                        TODO: Handle Subdirectories
        for(EgaArchiveFile f : files){            
            rootDirectory.add(new MemoryFile(f));
        }
    }

    /*
    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
        if (getPath(path) != null) {
            return -ErrorCodes.EEXIST();
        }
        MemoryPath parent = getParentPath(path);
        if (parent instanceof MemoryDirectory) {
            ((MemoryDirectory) parent).mkfile(getLastComponent(path));
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }
    */

    @Override
    public int getattr(String path, FileStat stat) {
        MemoryPath p = getPath(path);
        if (p != null) {
            p.getattr(stat);
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }

    private String getLastComponent(String path) {
        while (path.substring(path.length() - 1).equals("/")) {
            path = path.substring(0, path.length() - 1);
        }
        if (path.isEmpty()) {
            return "";
        }
        return path.substring(path.lastIndexOf("/") + 1);
    }

    private MemoryPath getParentPath(String path) {
        return rootDirectory.find(path.substring(0, path.lastIndexOf("/")));
    }

    private MemoryPath getPath(String path) {
        return rootDirectory.find(path);
    }


    @Override
    public int mkdir(String path, @mode_t long mode) {
        if (getPath(path) != null) {
            return -ErrorCodes.EEXIST();
        }
        MemoryPath parent = getParentPath(path);
        if (parent instanceof MemoryDirectory) {
            ((MemoryDirectory) parent).mkdir(getLastComponent(path));
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }


    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryFile)) {
            return -ErrorCodes.EISDIR();
        }
        return ((MemoryFile) p).read(buf, size, offset);
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryDirectory)) {
            return -ErrorCodes.ENOTDIR();
        }
        filter.apply(buf, ".", null, 0);
        filter.apply(buf, "..", null, 0);
        ((MemoryDirectory) p).read(buf, filter);
        return 0;
    }

    @Override
    public int rename(String path, String newName) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        MemoryPath newParent = getParentPath(newName);
        if (newParent == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(newParent instanceof MemoryDirectory)) {
            return -ErrorCodes.ENOTDIR();
        }
        p.delete();
        p.rename(newName.substring(newName.lastIndexOf("/")));
        ((MemoryDirectory) newParent).add(p);
        return 0;
    }

    @Override
    public int rmdir(String path) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryDirectory)) {
            return -ErrorCodes.ENOTDIR();
        }
        p.delete();
        return 0;
    }

    /*
    @Override
    public int truncate(String path, long offset) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryFile)) {
            return -ErrorCodes.EISDIR();
        }
        ((MemoryFile) p).truncate(offset);
        return 0;
    }
    */

    @Override
    public int unlink(String path) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        p.delete();
        return 0;
    }

    /*
    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryFile)) {
            return -ErrorCodes.EISDIR();
        }
        return ((MemoryFile) p).write(buf, size, offset);
    }
    */
    
    // Instantiate Cipher, etc.
    @Override
    public int open(String path, FuseFileInfo fi) {
        MemoryPath p = getPath(path);
        
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryFile)) {
            return -ErrorCodes.EISDIR();
        } else {
            int open = ( (MemoryFile)p ).open();
        }
    
        return 0;
    }
}

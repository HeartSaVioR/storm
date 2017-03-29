/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.daemon.JarTransformer;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import clojure.lang.Keyword;

public class DaemonUtils {
    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static DaemonUtils _instance = new DaemonUtils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     * @param u a DaemonUtils instance
     * @return the previously set instance
     */
    public static DaemonUtils setInstance(DaemonUtils u) {
        DaemonUtils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }

    public static final Logger LOG = LoggerFactory.getLogger(DaemonUtils.class);
    public static final String DEFAULT_BLOB_VERSION_SUFFIX = ".version";
    public static final String CURRENT_BLOB_SUFFIX_ID = "current";
    public static final String DEFAULT_CURRENT_BLOB_SUFFIX = "." + CURRENT_BLOB_SUFFIX_ID;

    public static final boolean IS_ON_WINDOWS = "Windows_NT".equals(System.getenv("OS"));
    public static final String FILE_PATH_SEPARATOR = System.getProperty("file.separator");
    public static final String CLASS_PATH_SEPARATOR = System.getProperty("path.separator");

    public static final int SIGKILL = 9;
    public static final int SIGTERM = 15;

    public static JarTransformer jarTransformer(String klass) {
        JarTransformer ret = null;
        if (klass != null) {
            ret = (JarTransformer) ReflectionUtils.newInstance(klass);
        }
        return ret;
    }

    public static byte[] toCompressedJsonConf(Map<String, Object> stormConf) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            OutputStreamWriter out = new OutputStreamWriter(new GZIPOutputStream(bos));
            JSONValue.writeJSONString(stormConf, out);
            out.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static Localizer createLocalizer(Map conf, String baseDir) {
        return new Localizer(conf, baseDir);
    }

    public static ClientBlobStore getClientBlobStoreForSupervisor(Map conf) {
        ClientBlobStore store = (ClientBlobStore) ReflectionUtils.newInstance(
                (String) conf.get(DaemonConfig.SUPERVISOR_BLOBSTORE));
        store.prepare(conf);
        return store;
    }

    public static BlobStore getNimbusBlobStore(Map conf, NimbusInfo nimbusInfo) {
        return getNimbusBlobStore(conf, null, nimbusInfo);
    }

    public static BlobStore getNimbusBlobStore(Map conf, String baseDir, NimbusInfo nimbusInfo) {
        String type = (String)conf.get(DaemonConfig.NIMBUS_BLOBSTORE);
        if (type == null) {
            type = LocalFsBlobStore.class.getName();
        }
        BlobStore store = (BlobStore) ReflectionUtils.newInstance(type);
        HashMap nconf = new HashMap(conf);
        // only enable cleanup of blobstore on nimbus
        nconf.put(DaemonConfig.BLOBSTORE_CLEANUP_ENABLE, Boolean.TRUE);

        if(store != null) {
            // store can be null during testing when mocking utils.
            store.prepare(nconf, baseDir, nimbusInfo);
        }
        return store;
    }

    /**
     * Meant to be called only by the supervisor for stormjar/stormconf/stormcode files.
     * @param key
     * @param localFile
     * @param cb
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     * @throws IOException
     */
    public static void downloadResourcesAsSupervisor(String key, String localFile,
                                                     ClientBlobStore cb) throws AuthorizationException, KeyNotFoundException, IOException {
        _instance.downloadResourcesAsSupervisorImpl(key, localFile, cb);
    }

    public static boolean checkFileExists(File path) {
        return Files.exists(path.toPath());
    }

    public static boolean checkFileExists(String dir, String file) {
        return Utils.checkFileExists(dir + Utils.FILE_PATH_SEPARATOR + file);
    }

    public void downloadResourcesAsSupervisorImpl(String key, String localFile,
            ClientBlobStore cb) throws AuthorizationException, KeyNotFoundException, IOException {
        final int MAX_RETRY_ATTEMPTS = 2;
        final int ATTEMPTS_INTERVAL_TIME = 100;
        for (int retryAttempts = 0; retryAttempts < MAX_RETRY_ATTEMPTS; retryAttempts++) {
            if (downloadResourcesAsSupervisorAttempt(cb, key, localFile)) {
                break;
            }
            Utils.sleep(ATTEMPTS_INTERVAL_TIME);
        }
    }

    private static boolean downloadResourcesAsSupervisorAttempt(ClientBlobStore cb, String key, String localFile) {
        boolean isSuccess = false;
        try (FileOutputStream out = new FileOutputStream(localFile);
                InputStreamWithMeta in = cb.getBlob(key);) {
            long fileSize = in.getFileLength();

            byte[] buffer = new byte[1024];
            int len;
            int downloadFileSize = 0;
            while ((len = in.read(buffer)) >= 0) {
                out.write(buffer, 0, len);
                downloadFileSize += len;
            }

            isSuccess = (fileSize == downloadFileSize);
        } catch (TException | IOException e) {
            LOG.error("An exception happened while downloading {} from blob store.", localFile, e);
        }
        if (!isSuccess) {
            try {
                Files.deleteIfExists(Paths.get(localFile));
            } catch (IOException ex) {
                LOG.error("Failed trying to delete the partially downloaded {}", localFile, ex);
            }
        }
        return isSuccess;
    }

    public static boolean CheckDirExists(String dir) {
        File file = new File(dir);
        return file.isDirectory();
    }

    public static long nimbusVersionOfBlob(String key, ClientBlobStore cb) throws AuthorizationException, KeyNotFoundException {
        long nimbusBlobVersion = 0;
        ReadableBlobMeta metadata = cb.getBlobMeta(key);
        nimbusBlobVersion = metadata.get_version();
        return nimbusBlobVersion;
    }

    public static String getFileOwner(String path) throws IOException {
        return Files.getOwner(FileSystems.getDefault().getPath(path)).getName();
    }

    public static long localVersionOfBlob(String localFile) {
        File f = new File(localFile + DEFAULT_BLOB_VERSION_SUFFIX);
        long currentVersion = 0;
        if (f.exists() && !(f.isDirectory())) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(f));
                String line = br.readLine();
                currentVersion = Long.parseLong(line);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (br != null) {
                        br.close();
                    }
                } catch (Exception ignore) {
                    LOG.error("Exception trying to cleanup", ignore);
                }
            }
            return currentVersion;
        } else {
            return -1;
        }
    }

    public static String constructBlobWithVersionFileName(String fileName, long version) {
        return fileName + "." + version;
    }

    public static String constructBlobCurrentSymlinkName(String fileName) {
        return fileName + DaemonUtils.DEFAULT_CURRENT_BLOB_SUFFIX;
    }

    public static String constructVersionFileName(String fileName) {
        return fileName + DaemonUtils.DEFAULT_BLOB_VERSION_SUFFIX;
    }
    // only works on operating  systems that support posix
    public static void restrictPermissions(String baseDir) {
        try {
            Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>(
                    Arrays.asList(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                            PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ,
                            PosixFilePermission.GROUP_EXECUTE));
            Files.setPosixFilePermissions(FileSystems.getDefault().getPath(baseDir), perms);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Unpack matching files from a jar. Entries inside the jar that do
     * not match the given pattern will be skipped.
     *
     * @param jarFile the .jar file to unpack
     * @param toDir the destination directory into which to unpack the jar
     */
    public static void unJar(File jarFile, File toDir)
            throws IOException {
        JarFile jar = new JarFile(jarFile);
        try {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = jar.getInputStream(entry);
                    try {
                        File file = new File(toDir, entry.getName());
                        ensureDirectory(file.getParentFile());
                        OutputStream out = new FileOutputStream(file);
                        try {
                            copyBytes(in, out, 8192);
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            jar.close();
        }
    }

    /**
     * Copies from one stream to another.
     *
     * @param in InputStream to read from
     * @param out OutputStream to write to
     * @param buffSize the size of the buffer
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize)
            throws IOException {
        PrintStream ps = out instanceof PrintStream ? (PrintStream)out : null;
        byte buf[] = new byte[buffSize];
        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {
            out.write(buf, 0, bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
            bytesRead = in.read(buf);
        }
    }

    /**
     * Ensure the existence of a given directory.
     *
     * @throws IOException if it cannot be created and does not already exist
     */
    private static void ensureDirectory(File dir) throws IOException {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " +
                    dir.toString());
        }
    }

    /**
     * Given a Tar File as input it will untar the file in a the untar directory
     * passed as the second parameter
     * <p/>
     * This utility will untar ".tar" files and ".tar.gz","tgz" files.
     *
     * @param inFile   The tar file as input.
     * @param untarDir The untar directory where to untar the tar file.
     * @throws IOException
     */
    public static void unTar(File inFile, File untarDir) throws IOException {
        if (!untarDir.mkdirs()) {
            if (!untarDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create " + untarDir);
            }
        }

        boolean gzipped = inFile.toString().endsWith("gz");
        if (Utils.isOnWindows()) {
            // Tar is not native to Windows. Use simple Java based implementation for
            // tests and simple tar archives
            unTarUsingJava(inFile, untarDir, gzipped);
        } else {
            // spawn tar utility to untar archive for full fledged unix behavior such
            // as resolving symlinks in tar archives
            unTarUsingTar(inFile, untarDir, gzipped);
        }
    }

    private static void unTarUsingTar(File inFile, File untarDir,
                                      boolean gzipped) throws IOException {
        StringBuffer untarCommand = new StringBuffer();
        if (gzipped) {
            untarCommand.append(" gzip -dc '");
            untarCommand.append(inFile.toString());
            untarCommand.append("' | (");
        }
        untarCommand.append("cd '");
        untarCommand.append(untarDir.toString());
        untarCommand.append("' ; ");
        untarCommand.append("tar -xf ");

        if (gzipped) {
            untarCommand.append(" -)");
        } else {
            untarCommand.append(inFile.toString());
        }
        String[] shellCmd = {"bash", "-c", untarCommand.toString()};
        ShellUtils.ShellCommandExecutor shexec = new ShellUtils.ShellCommandExecutor(shellCmd);
        shexec.execute();
        int exitcode = shexec.getExitCode();
        if (exitcode != 0) {
            throw new IOException("Error untarring file " + inFile +
                    ". Tar process exited with exit code " + exitcode);
        }
    }

    private static void unTarUsingJava(File inFile, File untarDir,
                                       boolean gzipped) throws IOException {
        InputStream inputStream = null;
        try {
            if (gzipped) {
                inputStream = new BufferedInputStream(new GZIPInputStream(
                        new FileInputStream(inFile)));
            } else {
                inputStream = new BufferedInputStream(new FileInputStream(inFile));
            }
            try (TarArchiveInputStream tis = new TarArchiveInputStream(inputStream)) {
                for (TarArchiveEntry entry = tis.getNextTarEntry(); entry != null; ) {
                    unpackEntries(tis, entry, untarDir);
                    entry = tis.getNextTarEntry();
                }
            }
        } finally {
            if(inputStream != null) {
                inputStream.close();
            }
        }
    }

    private static void unpackEntries(TarArchiveInputStream tis,
                                      TarArchiveEntry entry, File outputDir) throws IOException {
        if (entry.isDirectory()) {
            File subDir = new File(outputDir, entry.getName());
            if (!subDir.mkdirs() && !subDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create tar internal dir "
                        + outputDir);
            }
            for (TarArchiveEntry e : entry.getDirectoryEntries()) {
                unpackEntries(tis, e, subDir);
            }
            return;
        }
        File outputFile = new File(outputDir, entry.getName());
        if (!outputFile.getParentFile().exists()) {
            if (!outputFile.getParentFile().mkdirs()) {
                throw new IOException("Mkdirs failed to create tar internal dir "
                                      + outputDir);
            }
        }
        int count;
        byte data[] = new byte[2048];
        BufferedOutputStream outputStream = new BufferedOutputStream(
                new FileOutputStream(outputFile));

        while ((count = tis.read(data)) != -1) {
            outputStream.write(data, 0, count);
        }
        outputStream.flush();
        outputStream.close();
    }

    public static boolean isAbsolutePath(String path) {
        return Paths.get(path).isAbsolute();
    }

    public static void unpack(File localrsrc, File dst) throws IOException {
        String lowerDst = localrsrc.getName().toLowerCase();
        if (lowerDst.endsWith(".jar")) {
            unJar(localrsrc, dst);
        } else if (lowerDst.endsWith(".zip")) {
            unZip(localrsrc, dst);
        } else if (lowerDst.endsWith(".tar.gz") ||
                lowerDst.endsWith(".tgz") ||
                lowerDst.endsWith(".tar")) {
            unTar(localrsrc, dst);
        } else {
            LOG.warn("Cannot unpack " + localrsrc);
            if (!localrsrc.renameTo(dst)) {
                throw new IOException("Unable to rename file: [" + localrsrc
                        + "] to [" + dst + "]");
            }
        }
        if (localrsrc.isFile()) {
            localrsrc.delete();
        }
    }

    public static boolean canUserReadBlob(ReadableBlobMeta meta, String user) {
        SettableBlobMeta settable = meta.get_settable();
        for (AccessControl acl : settable.get_acl()) {
            if (acl.get_type().equals(AccessControlType.OTHER) && (acl.get_access() & BlobStoreAclHandler.READ) > 0) {
                return true;
            }
            if (acl.get_name().equals(user) && (acl.get_access() & BlobStoreAclHandler.READ) > 0) {
                return true;
            }
        }
        return false;
    }

    public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
        int base = sum / numPieces;
        int numInc = sum % numPieces;
        int numBases = numPieces - numInc;
        TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
        ret.put(base, numBases);
        if (numInc != 0) {
            ret.put(base+1, numInc);
        }
        return ret;
    }

    /**
     * Is the cluster configured to interact with ZooKeeper in a secure way?
     * This only works when called from within Nimbus or a Supervisor process.
     * @param conf the storm configuration, not the topology configuration
     * @return true if it is configured else false.
     */
    public static boolean isZkAuthenticationConfiguredStormServer(Map conf) {
        return null != System.getProperty("java.security.auth.login.config")
                || (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME) != null
                && !((String)conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME)).isEmpty());
    }

    /**
     * Takes an input dir or file and returns the disk usage on that local directory.
     * Very basic implementation.
     *
     * @param dir The input dir to get the disk space of this local dir
     * @return The total disk space of the input local directory
     */
    public static long getDU(File dir) {
        long size = 0;
        if (!dir.exists())
            return 0;
        if (!dir.isDirectory()) {
            return dir.length();
        } else {
            File[] allFiles = dir.listFiles();
            if(allFiles != null) {
                for (int i = 0; i < allFiles.length; i++) {
                    boolean isSymLink;
                    try {
                        isSymLink = org.apache.commons.io.FileUtils.isSymlink(allFiles[i]);
                    } catch(IOException ioe) {
                        isSymLink = true;
                    }
                    if(!isSymLink) {
                        size += getDU(allFiles[i]);
                    }
                }
            }
            return size;
        }
    }

    /**
     * Gets some information, including stack trace, for a running thread.
     * @return A human-readable string of the dump.
     */
    public static String threadDump() {
        final StringBuilder dump = new StringBuilder();
        final java.lang.management.ThreadMXBean threadMXBean =  java.lang.management.ManagementFactory.getThreadMXBean();
        final java.lang.management.ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        for (java.lang.management.ThreadInfo threadInfo : threadInfos) {
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            dump.append("\n   lock: ");
            dump.append(threadInfo.getLockName());
            dump.append(" owner: ");
            dump.append(threadInfo.getLockOwnerName());
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n   java.lang.Thread.State: ");
            dump.append(state);
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

    /**
     * Given a File input it will unzip the file in a the unzip directory
     * passed as the second parameter
     * @param inFile The zip file as input
     * @param unzipDir The unzip directory where to unzip the zip file.
     * @throws IOException
     */
    public static void unZip(File inFile, File unzipDir) throws IOException {
        Enumeration<? extends ZipEntry> entries;
        ZipFile zipFile = new ZipFile(inFile);

        try {
            entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    InputStream in = zipFile.getInputStream(entry);
                    try {
                        File file = new File(unzipDir, entry.getName());
                        if (!file.getParentFile().mkdirs()) {
                            if (!file.getParentFile().isDirectory()) {
                                throw new IOException("Mkdirs failed to create " +
                                                      file.getParentFile().toString());
                            }
                        }
                        OutputStream out = new FileOutputStream(file);
                        try {
                            byte[] buffer = new byte[8192];
                            int i;
                            while ((i = in.read(buffer)) != -1) {
                                out.write(buffer, 0, i);
                            }
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            zipFile.close();
        }
    }

    /**
     * Given a zip File input it will return its size
     * Only works for zip files whose uncompressed size is less than 4 GB,
     * otherwise returns the size module 2^32, per gzip specifications
     * @param myFile The zip file as input
     * @throws IOException
     * @return zip file size as a long
     */
    public static long zipFileSize(File myFile) throws IOException{
        RandomAccessFile raf = new RandomAccessFile(myFile, "r");
        raf.seek(raf.length() - 4);
        long b4 = raf.read();
        long b3 = raf.read();
        long b2 = raf.read();
        long b1 = raf.read();
        long val = (b1 << 24) | (b2 << 16) + (b3 << 8) + b4;
        raf.close();
        return val;
    }

    public static int getAvailablePort(int prefferedPort) {
        int localPort = -1;
        try(ServerSocket socket = new ServerSocket(prefferedPort)) {
            localPort = socket.getLocalPort();
        } catch(IOException exp) {
            if (prefferedPort > 0) {
                return getAvailablePort(0);
            }
        }
        return localPort;
    }

    public static int getAvailablePort() {
        return getAvailablePort(0);
    }

    /**
     * Determines if a zip archive contains a particular directory.
     *
     * @param zipfile path to the zipped file
     * @param target directory being looked for in the zip.
     * @return boolean whether or not the directory exists in the zip.
     */
    public static boolean zipDoesContainDir(String zipfile, String target) throws IOException {
        List<ZipEntry> entries = (List<ZipEntry>) Collections.list(new ZipFile(zipfile).entries());

        String targetDir = target + "/";
        for(ZipEntry entry : entries) {
            String name = entry.getName();
            if(name.startsWith(targetDir)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Joins any number of maps together into a single map, combining their values into
     * a list, maintaining values in the order the maps were passed in. Nulls are inserted
     * for given keys when the map does not contain that key.
     *
     * i.e. joinMaps({'a' => 1, 'b' => 2}, {'b' => 3}, {'a' => 4, 'c' => 5}) ->
     *      {'a' => [1, null, 4], 'b' => [2, 3, null], 'c' => [null, null, 5]}
     *
     * @param maps variable number of maps to join - order affects order of values in output.
     * @return combined map
     */
    public static <K, V> Map<K, List<V>> joinMaps(Map<K, V>... maps) {
        Map<K, List<V>> ret = new HashMap<>();

        Set<K> keys = new HashSet<>();

        for(Map<K, V> map : maps) {
            keys.addAll(map.keySet());
        }

        for(Map<K, V> m : maps) {
            for(K key : keys) {
                V value = m.get(key);

                if(!ret.containsKey(key)) {
                    ret.put(key, new ArrayList<V>());
                }

                List<V> targetList = ret.get(key);
                targetList.add(value);
            }
        }
        return ret;
    }

    /**
     * Fills up chunks out of a collection (given a maximum amount of chunks)
     *
     * i.e. partitionFixed(5, [1,2,3]) -> [[1,2,3]]
     *      partitionFixed(5, [1..9]) -> [[1,2], [3,4], [5,6], [7,8], [9]]
     *      partitionFixed(3, [1..10]) -> [[1,2,3,4], [5,6,7], [8,9,10]]
     * @param maxNumChunks the maximum number of chunks to return
     * @param coll the collection to be chunked up
     * @return a list of the chunks, which are themselves lists.
     */
    public static <T> List<List<T>> partitionFixed(int maxNumChunks, Collection<T> coll) {
        List<List<T>> ret = new ArrayList<>();

        if(maxNumChunks == 0 || coll == null) {
            return ret;
        }

        Map<Integer, Integer> parts = integerDivided(coll.size(), maxNumChunks);

        // Keys sorted in descending order
        List<Integer> sortedKeys = new ArrayList<Integer>(parts.keySet());
        Collections.sort(sortedKeys, Collections.reverseOrder());


        Iterator<T> it = coll.iterator();
        for(Integer chunkSize : sortedKeys) {
            if(!it.hasNext()) { break; }
            Integer times = parts.get(chunkSize);
            for(int i = 0; i < times; i++) {
                if(!it.hasNext()) { break; }
                List<T> chunkList = new ArrayList<>();
                for(int j = 0; j < chunkSize; j++) {
                    if(!it.hasNext()) { break; }
                    chunkList.add(it.next());
                }
                ret.add(chunkList);
            }
        }

        return ret;
    }

    /**
     * Return a new instance of a pluggable specified in the conf.
     * @param conf The conf to read from.
     * @param configKey The key pointing to the pluggable class
     * @return an instance of the class or null if it is not specified.
     */
    public static Object getConfiguredClass(Map conf, Object configKey) {
        if (conf.containsKey(configKey)) {
            return ReflectionUtils.newInstance((String)conf.get(configKey));
        }
        return null;
    }

    public static String logsFilename(String stormId, String port) {
        return stormId + FILE_PATH_SEPARATOR + port + FILE_PATH_SEPARATOR + "worker.log";
    }

    public static String eventLogsFilename(String stormId, String port) {
        return stormId + FILE_PATH_SEPARATOR + port + FILE_PATH_SEPARATOR + "events.log";
    }

    public static Object readYamlFile(String yamlFile) {
        try (FileReader reader = new FileReader(yamlFile)) {
            return new Yaml(new SafeConstructor()).load(reader);
        } catch(Exception ex) {
            LOG.error("Failed to read yaml file.", ex);
        }
        return null;
    }

    /**
     * Make sure a given key name is valid for the storm config.
     * Throw RuntimeException if the key isn't valid.
     * @param name The name of the config key to check.
     */
    private static final Set<String> disallowedKeys = new HashSet<>(Arrays.asList(new String[] {"/", ".", ":", "\\"}));
    public static void validateKeyName(String name) {

        for(String key : disallowedKeys) {
            if( name.contains(key) ) {
                throw new RuntimeException("Key name cannot contain any of the following: " + disallowedKeys.toString());
            }
        }
        if(name.trim().isEmpty()) {
            throw new RuntimeException("Key name cannot be blank");
        }
    }

    /**
     * Find the first item of coll for which pred.test(...) returns true.
     * @param pred The IPredicate to test for
     * @param coll The Collection of items to search through.
     * @return The first matching value in coll, or null if nothing matches.
     */
    public static <T> T findOne (IPredicate<T> pred, Collection<T> coll) {
        if(coll == null) {
            return null;
        }
        for(T elem : coll) {
            if (pred.test(elem)) {
                return elem;
            }
        }
        return null;
    }

    public static <T, U> T findOne (IPredicate<T> pred, Map<U, T> map) {
        if(map == null) {
            return null;
        }
        return findOne(pred, (Set<T>) map.entrySet());
    }


    public static int execCommand(String... command) throws ExecuteException, IOException {
        CommandLine cmd = new CommandLine(command[0]);
        for (int i = 1; i < command.length; i++) {
            cmd.addArgument(command[i]);
        }

        DefaultExecutor exec = new DefaultExecutor();
        return exec.execute(cmd);
    }

    /**
     * Extract dir from the jar to destdir
     *
     * @param jarpath Path to the jar file
     * @param dir Directory in the jar to pull out
     * @param destdir Path to the directory where the extracted directory will be put
     */
    public static void extractDirFromJar(String jarpath, String dir, File destdir) {
        _instance.extractDirFromJarImpl(jarpath, dir, destdir);
    }
    
    public void extractDirFromJarImpl(String jarpath, String dir, File destdir) {
        try (JarFile jarFile = new JarFile(jarpath)) {
            Enumeration<JarEntry> jarEnums = jarFile.entries();
            while (jarEnums.hasMoreElements()) {
                JarEntry entry = jarEnums.nextElement();
                if (!entry.isDirectory() && entry.getName().startsWith(dir)) {
                    File aFile = new File(destdir, entry.getName());
                    aFile.getParentFile().mkdirs();
                    try (FileOutputStream out = new FileOutputStream(aFile);
                         InputStream in = jarFile.getInputStream(entry)) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        } catch (IOException e) {
            LOG.info("Could not extract {} from {}", dir, jarpath);
        }
    }

    public static void sendSignalToProcess(long lpid, int signum) throws IOException {
        String pid = Long.toString(lpid);
        try {
            if (Utils.isOnWindows()) {
                if (signum == SIGKILL) {
                    execCommand("taskkill", "/f", "/pid", pid);
                } else {
                    execCommand("taskkill", "/pid", pid);
                }
            } else {
                execCommand("kill", "-" + signum, pid);
            }
        } catch (ExecuteException e) {
            LOG.info("Error when trying to kill {}. Process is probably already dead.", pid);
        } catch (IOException e) {
            LOG.info("IOException Error when trying to kill {}.", pid);
            throw e;
        }
    }

    public static void forceKillProcess (String pid) throws IOException {
        sendSignalToProcess(Long.parseLong(pid), SIGKILL);
    }

    public static void killProcessWithSigTerm (String pid) throws IOException {
        sendSignalToProcess(Long.parseLong(pid), SIGTERM);
    }

    /**
     * Returns the combined string, escaped for posix shell.
     * @param command the list of strings to be combined
     * @return the resulting command string
     */
    public static String shellCmd (List<String> command) {
        List<String> changedCommands = new ArrayList<>(command.size());
        for (String str: command) {
            if (str == null) {
                continue;
            }
            changedCommands.add("'" + str.replaceAll("'", "'\"'\"'") + "'");
        }
        return StringUtils.join(changedCommands, " ");
    }

    public static String scriptFilePath (String dir) {
        return dir + FILE_PATH_SEPARATOR + "storm-worker-script.sh";
    }

    public static String containerFilePath (String dir) {
        return dir + FILE_PATH_SEPARATOR + "launch_container.sh";
    }

    public static double nullToZero (Double v) {
        return (v != null ? v : 0);
    }

    /**
     * Returns a Collection of file names found under the given directory.
     * @param dir a directory
     * @return the Collection of file names
     */
    public static Collection<String> readDirContents(String dir) {
        Collection<String> ret = new HashSet<>();
        File[] files = new File(dir).listFiles();
        if (files != null) {
            for (File f: files) {
                ret.add(f.getName());
            }
        }
        return ret;
    }

    /**
     * Returns the value of java.class.path System property. Kept separate for
     * testing.
     * @return the classpath
     */
    public static String currentClasspath() {
        return _instance.currentClasspathImpl();
    }

    // Non-static impl methods exist for mocking purposes.
    public String currentClasspathImpl() {
        return System.getProperty("java.class.path");
    }

    public static String addToClasspath(String classpath,
                Collection<String> paths) {
        return _instance.addToClasspathImpl(classpath, paths);
    }

    public static String addToClasspath(Collection<String> classpaths,
                Collection<String> paths) {
        return _instance.addToClasspathImpl(classpaths, paths);
    }

    // Non-static impl methods exist for mocking purposes.
    public String addToClasspathImpl(String classpath,
                Collection<String> paths) {
        if (paths == null || paths.isEmpty()) {
            return classpath;
        }
        List<String> l = new LinkedList<>();
        l.add(classpath);
        l.addAll(paths);
        return StringUtils.join(l, CLASS_PATH_SEPARATOR);
    }

    public String addToClasspathImpl(Collection<String> classpaths,
                Collection<String> paths) {
        List<String> allPaths = new ArrayList<>();
        if(classpaths != null) {
            allPaths.addAll(classpaths);
        }
        if(paths != null) {
            allPaths.addAll(paths);
        }
        return StringUtils.join(allPaths, CLASS_PATH_SEPARATOR);
    }

    /**
     * a or b the first one that is not null
     * @param a something
     * @param b something else
     * @return a or b the first one that is not null
     */
    public static <V> V OR(V a, V b) {
        return a == null ? b : a;
    }

    /**
     * Writes a posix shell script file to be executed in its own process.
     * @param dir the directory under which the script is to be written
     * @param command the command the script is to execute
     * @param environment optional environment variables to set before running the script's command. May be  null.
     * @return the path to the script that has been written
     */
    public static String writeScript(String dir, List<String> command,
                                     Map<String,String> environment) throws IOException {
        String path = DaemonUtils.scriptFilePath(dir);
        try(BufferedWriter out = new BufferedWriter(new FileWriter(path))) {
            out.write("#!/bin/bash");
            out.newLine();
            if (environment != null) {
                for (String k : environment.keySet()) {
                    String v = environment.get(k);
                    if (v == null) {
                        v = "";
                    }
                    out.write(DaemonUtils.shellCmd(
                            Arrays.asList(
                                    "export",k+"="+v)));
                    out.write(";");
                    out.newLine();
                }
            }
            out.newLine();
            out.write("exec "+ DaemonUtils.shellCmd(command)+";");
        }
        return path;
    }

    public static <T> List<T> interleaveAll(List<List<T>> nodeList) {
        if (nodeList != null && nodeList.size() > 0) {
            List<T> first = new ArrayList<T>();
            List<List<T>> rest = new ArrayList<List<T>>();
            for (List<T> node : nodeList) {
                if (node != null && node.size() > 0) {
                  first.add(node.get(0));
                  rest.add(node.subList(1, node.size()));
                }
            }
            List<T> interleaveRest = interleaveAll(rest);
            if (interleaveRest != null) {
                first.addAll(interleaveRest);
            }
            return first;
        }
        return null;
      }

    /**
     * converts a clojure PersistentMap to java HashMap
     */
    public static Map<String, Object> convertClojureMapToJavaMap(Map map) {
        Map<String, Object> ret = new HashMap<>(map.size());
        for (Object obj : map.entrySet()) {
            Map.Entry entry = (Map.Entry) obj;
            Keyword keyword = (Keyword) entry.getKey();
            String key = keyword.getName();
            if (key.startsWith(":")) {
                key = key.substring(1, key.length());
            }
            Object value = entry.getValue();
            ret.put(key, value);
        }

        return ret;
    }
}

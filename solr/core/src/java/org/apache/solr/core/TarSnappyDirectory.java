package org.apache.solr.core;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.Future;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.xerial.snappy.SnappyFramedOutputStream;

/** A straightforward implementation of {@link FSDirectory}
 *  using {@link Files#newByteChannel(Path, java.nio.file.OpenOption...)}.  
 *  However, this class has
 *  poor concurrent performance (multiple threads will
 *  bottleneck) as it synchronizes when multiple threads
 *  read from the same file.  It's usually better to use
 *  {@link NIOFSDirectory} or {@link MMapDirectory} instead.
 * <p>
 * <b>NOTE:</b> Accessing this class either directly or
 * indirectly from a thread while it's interrupted can close the
 * underlying file descriptor immediately if at the same time the thread is
 * blocked on IO. The file descriptor will remain closed and subsequent access
 * to {@link TarSnappyDirectory} will throw a {@link ClosedChannelException}. If
 * your application uses either {@link Thread#interrupt()} or
 * {@link Future#cancel(boolean)} you should use the legacy {@code RAFDirectory}
 * from the Lucene {@code misc} module in favor of {@link TarSnappyDirectory}.
 * </p>
 */
public class TarSnappyDirectory extends BaseDirectory {

  public static String ARCHIVE_SUFFIX = "tar.sz";

  private String archiveName;
  private File archiveFile;
  private TarArchiveOutputStream oStream;

  /** Create a new SimpleFSDirectory for the named location.
   *  The directory is created at the named location if it does not yet exist.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use
   * @throws IOException if there is a low-level I/O error
   */
  public TarSnappyDirectory(Path path, LockFactory lockFactory) throws IOException {
    super(lockFactory);

    archiveName = path.toFile().getName();
    archiveFile = new File(archiveName + "." + ARCHIVE_SUFFIX);

    BufferedOutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(archiveFile));
    SnappyFramedOutputStream snappyOutputStream = new SnappyFramedOutputStream(fileOutputStream);
    oStream = new TarArchiveOutputStream(snappyOutputStream);
  }

  /** Create a new TarSnappyDirectory for the named location and {@link FSLockFactory#getDefault()}.
   *  The directory is created at the named location if it does not yet exist.
   *
   * @param path the path of the directory
   * @throws IOException if there is a low-level I/O error
   */
  public TarSnappyDirectory(Path path) throws IOException {
    this(path, FSLockFactory.getDefault());
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    throw new UnsupportedOperationException("openInput not supported for TarSnappyDirectoryFactory");
  }

  @Override
  public String[] listAll() throws IOException {
    throw new UnsupportedOperationException("listAll not supported for TarSnappyDirectoryFactory");
  }

  @Override
  public void deleteFile(String name) throws IOException {
    throw new UnsupportedOperationException("deleteFile not supported for TarSnappyDirectoryFactory");
  }

  @Override
  public long fileLength(String name) throws IOException {
    throw new UnsupportedOperationException("fileLength not supported for TarSnappyDirectoryFactory");
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    throw new UnsupportedOperationException("sync not supported for TarSnappyDirectoryFactory");
  }

  @Override
  public void renameFile(String source, String dest) throws IOException {
    throw new UnsupportedOperationException("renameFile not supported for TarSnappyDirectoryFactory");
  }

  @Override
  public void close() throws IOException {
    oStream.flush();
    oStream.close();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return new TarEntryWriter(new File(archiveName, name).toPath(), oStream);
  }

}

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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.OutputStreamIndexOutput;

public class TarEntryWriter extends OutputStreamIndexOutput {
  private static final int BUFFER_SIZE = 8192;

  public TarEntryWriter(Path outputPath, TarArchiveOutputStream tarOutputStream, long outputSize) throws IOException {
    super("path=" + outputPath, getOutputStream(outputPath, tarOutputStream, outputSize), BUFFER_SIZE);
  }

  private static OutputStream getOutputStream(Path path, TarArchiveOutputStream tarOutputStream, long outputSize) throws IOException {
    String entryName = path.toString();
    TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(entryName);
    tarArchiveEntry.setSize(outputSize);
    tarOutputStream.putArchiveEntry(tarArchiveEntry);
    return new TarEntryWriterOutputStream(tarOutputStream);
  }
}

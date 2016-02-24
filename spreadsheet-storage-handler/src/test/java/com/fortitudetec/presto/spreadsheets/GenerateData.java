/**
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
package com.fortitudetec.presto.spreadsheets;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenerateData {

  private static final String SEP = new String(new char[] { (char) 1 });

  public static void main(String[] args) throws IOException {
    Configuration configuration = new Configuration();
    Path path = new Path("hdfs://node-000/user/hive/warehouse/test");
    FileSystem fileSystem = path.getFileSystem(configuration);
    OutputStream out = fileSystem.create(new Path(path, UUID.randomUUID().toString()));
    PrintWriter writer = new PrintWriter(out);
    Random random = new Random();
    for (int i = 0; i < 10000000; i++) {
      for (int c = 0; c < 5; c++) {
        if (c != 0) {
          writer.print(SEP);
        }
        writer.print(Long.toString(Math.abs(random.nextLong())));
      }
      writer.println();
    }
    writer.close();
  }

}

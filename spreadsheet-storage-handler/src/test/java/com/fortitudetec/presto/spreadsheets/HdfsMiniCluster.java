package com.fortitudetec.presto.spreadsheets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class HdfsMiniCluster {

  private static MiniDFSCluster cluster;
  private static File storePathDir = new File("./test");
  private static FileSystem fileSystem;

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();

    File file = new File("./tmp-conf/");
    file.mkdirs();
    writeConfiguration(new File(file, "hdfs-site.xml"));
    writeConfiguration(new File(file, "core-site.xml"));

    Path path = new Path("/user/amccurry/spreadsheets");
    String fileName = "Presto Example.xlsx";
    try (InputStream input = HdfsMiniCluster.class.getResourceAsStream("/" + fileName)) {
      try (FSDataOutputStream output = fileSystem.create(new Path(path, fileName), true)) {
        IOUtils.copy(input, output);
      }
    }

    System.out.println(fileSystem.getUri());
    Thread.sleep(100000000);
  }

  private static void writeConfiguration(File file) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      Configuration conf = fileSystem.getConf();
      conf.writeXml(outputStream);
    }
  }

}

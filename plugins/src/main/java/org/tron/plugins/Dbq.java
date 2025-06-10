package org.tron.plugins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.tron.plugins.utils.ByteArray;
import org.tron.plugins.utils.db.DBInterface;
import org.tron.plugins.utils.db.DBIterator;
import org.tron.plugins.utils.db.DbTool;
import picocli.CommandLine;

@Slf4j(topic = "qd")
@CommandLine.Command(name = "qd",
    description = "query data from db.",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0:Successful",
        "n:query failed,please check toolkit.log"})
public class Dbq implements Callable<Integer> {

  @CommandLine.Spec
  CommandLine.Model.CommandSpec spec;
  @CommandLine.Parameters(index = "0",
      description = " db path for query")
  private Path db;
  @CommandLine.Option(
      names = { "--keys"},
      description = "key for query in hex",
      split = ","
  )
  private List<String> keys = new ArrayList<>();

  @CommandLine.Option(names = {"-f", "--file"}, description = "File containing keys for query", required = false)
  private String keysFile;
  @CommandLine.Option(names = {"-h", "--help"}, help = true, description = "display a help message")
  private boolean help;

  @Override
  public Integer call() throws Exception {
    if (help) {
      spec.commandLine().usage(System.out);
      return 0;
    }
    if (!db.toFile().exists()) {
      logger.info(" {} does not exist.", db);
      spec.commandLine().getErr().println(spec.commandLine().getColorScheme()
          .errorText(String.format("%s does not exist.", db)));
      return 404;
    }
    if (keysFile != null) {
      // 从文件中读取keys，并填充到keys列表
      readKeysFromFile(keysFile);
    }
    return query();
  }


//  private int query() throws RocksDBException, IOException {
//    try (
//        DBInterface database  = DbTool.getDB(this.db.getParent(),
//            this.db.getFileName().toString())) {
//      if (keys != null && !keys.isEmpty()) {
//        keys.stream().map(ByteArray::fromHexString).forEach(k -> {
//          long start = System.nanoTime();
//          database.get(k);
//          long end = System.nanoTime();
//          spec.commandLine().getOut().format("耗时: %d μs", (end - start) / 1000).println();
//        });
//      }
//    }
//    return 0;
//  }

  private int query() throws RocksDBException, IOException {
    try (DBInterface database = DbTool.getDB(this.db.getParent(), this.db.getFileName().toString())) {
      DBIterator iterator = database.iterator();
      long start = System.nanoTime();
      int i = 0;
      for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
        if (i >= 1000000) {
          break;
        }
        iterator.getValue();
        i++;
      }
      long end = System.nanoTime();
      spec.commandLine().getOut().format("耗时: %d μs", (end - start) / 1000).println();
    }
    return 0;
  }

  private void readKeysFromFile(String filePath) throws IOException {
    File file = new File(filePath);
    if (!file.exists()) {
      spec.commandLine().getErr().println(spec.commandLine().getColorScheme()
          .errorText(String.format("文件 %s 不存在.", filePath)));
      return;
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty()) {
          keys.add(line);  // 将每行的key加入到keys列表中
        }
      }
    }
  }
}
package org.tron.plugins;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.tron.plugins.utils.ByteArray;
import org.tron.plugins.utils.db.DBInterface;
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
  @CommandLine.Option(names = { "--keys"},
       description = "key for query in hex")
  private List<String> keys;
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
    return query();
  }


  private int query() throws RocksDBException, IOException {
    try (
        DBInterface database  = DbTool.getDB(this.db.getParent(),
            this.db.getFileName().toString())) {
      if (keys != null && !keys.isEmpty()) {
        keys.stream().map(ByteArray::fromHexString).forEach(k -> {
          long start = System.nanoTime();
          database.get(k);
          long end = System.nanoTime();
          spec.commandLine().getOut().format("耗时: %d ms", (end - start) / 1000).println();
        });
      }
    }
    return 0;
  }
}
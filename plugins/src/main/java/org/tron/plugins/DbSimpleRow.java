package org.tron.plugins;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.tron.plugins.utils.DBUtils;
import org.tron.plugins.utils.FileUtils;
import picocli.CommandLine;


@CommandLine.Command(name = "sr",
    description = "simple db row .")
@Slf4j(topic = "sr")
public class DbSimpleRow implements Callable<Integer> {
  private static final int BATCH = 1024;
  public static final byte ADD_PRE_FIX_BYTE_MAINNET = (byte) 0x41;

  @CommandLine.Spec
  CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"-d", "--database"},
      defaultValue = "output-directory/database",
      description = "database directory path. Default: ${DEFAULT-VALUE}")
  private Path database;

  @CommandLine.Option(names = {"--new-database"},
      defaultValue = "new/database")
  private Path newDatabase;

  @CommandLine.Option(names = {"--sub-db"},
      defaultValue = "account",
      description = " sub db sub db name")
  private String subDb;


  @CommandLine.Option(names = {"-h", "--help"}, help = true, description = "display a help message")
  boolean help;

  final Random random = new Random();
  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");


  @Override
  public Integer call() throws Exception {
    if (help) {
      spec.commandLine().usage(System.out);
      return 0;
    }
    long start = System.currentTimeMillis();
    this.run();
    long cost = System.currentTimeMillis() - start;
    spec.commandLine().getOut().println(String.format("simple %s done,cost:%s seconds", subDb,
        cost / 1000));
    return 0;
  }

  private void run() throws Exception {
//    final Path sourcePath = Paths.get(database.toString(), targetDb);
    Path targetPath = Paths.get(this.newDatabase.toString(), subDb);
    FileUtils.createDirIfNotExists(targetPath.toString());
    DB source = DBUtils.newLevelDb(Paths.get(database.toString(), subDb));
    final DB newDb = DBUtils.newLevelDb(Paths.get(newDatabase.toString(), subDb));
    logger.info("Rewrite db {} start", subDb);
    spec.commandLine().getOut().println(String.format("%s Rewrite db %s start",
        dateFormat.format(new Date()), subDb));
    logger.info("DB size: {} M", getStats(source));
    spec.commandLine().getOut().println(String.format("%s DB size: %s M",
        dateFormat.format(new Date()), getStats(source)));
    merge(source, newDb);
    logger.info("simple db {} done", subDb);
    spec.commandLine().getOut().println(String.format("%s simple db %s done",
        dateFormat.format(new Date()), subDb));
    source.close();
    newDb.close();
  }


  public double getStats(DB db) {
    return Arrays.stream(db.getProperty("leveldb.stats").split("\n"))
        .skip(3)
        .map(s -> s.trim().replaceAll(" +", ",").split(",")[2])
        .mapToLong(Long::parseLong)
        .sum();
  }

  //todo
  public void merge(DB source, DB target) throws Exception {
    List<byte[]> keys = new ArrayList<>(BATCH);
    List<byte[]> values = new ArrayList<>(BATCH);
    JniDBFactory.pushMemoryPool(2048 * 2048);
    try (
        DBIterator levelIterator = source.iterator(new ReadOptions().fillCache(false))) {
      levelIterator.seekToFirst();
      while (levelIterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = levelIterator.next();
        byte[] key = entry.getKey();
        int randomVal = random.nextInt(10000);
        byte[] value = ByteBuffer.allocate(4).putInt(randomVal).array();
        keys.add(key);
        values.add(value);
        if (keys.size() >= BATCH) {
          insertToLevelDb(target, keys, values);
        }
      }
      // clear
      if (!keys.isEmpty()) {
        insertToLevelDb(target, keys, values);
      }
    } finally {
      JniDBFactory.popMemoryPool();
    }
  }


  private byte[] generateAddress() {
    // generate the random number
    if ("account".equalsIgnoreCase(subDb)) {
      byte[] result = new byte[21];
      random.nextBytes(result);
      result[0] = ADD_PRE_FIX_BYTE_MAINNET;
      return result;
    }
    if ("storage-row".equalsIgnoreCase(subDb)) {
      byte[] result = new byte[32];
      random.nextBytes(result);
      result[0] = ADD_PRE_FIX_BYTE_MAINNET;
      return result;
    }
    throw new IllegalArgumentException("Unsupported db type: " + subDb);
  }

  private void insertToLevelDb(DB db, List<byte[]> keys, List<byte[]> values)
      throws IOException {
    try (WriteBatch batch = db.createWriteBatch()) {
      for (int i = 0; i < keys.size(); i++) {
        byte[] k = keys.get(i);
        byte[] v = values.get(i);
        batch.put(k, v);
      }
      db.write(batch);
      keys.clear();
      values.clear();
    }
  }
}

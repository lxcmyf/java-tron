package org.tron.program;


import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.tron.program.DBConvert.newDefaultLevelDbOptions;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.exception.BadItemException;

@Slf4j(topic = "DB")
public class ScanDB {
  public static void main(String[] args) throws IOException {

    String sourcePath = args[0];

    iterateDB(sourcePath, bw);

  }


  public static void iterateDB(String sourcePath) throws IOException {
    Options options = newDefaultLevelDbOptions();
    DB db = initDB(sourcePath, options);
    try (DBIterator iterator = db.iterator()) {
      long maxNum = 0;
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      for (iterator.seekToLast(); iterator.hasPrev(); iterator.prev()) {
        byte[] value = iterator.peekPrev().getValue();
        BlockCapsule blockCapsule = new BlockCapsule(value);
        long num = blockCapsule.getBlockId().getNum();
        long timeStamp = blockCapsule.getTimeStamp();
        String dateString = sdf.format(new Date(timeStamp));
        maxNum = Math.max(maxNum, num);
        // 46257710
        // 55715000
        // 45391198
        if (num <= 69833743) {
          break;
        }
        if (num <= 69891327) {
          List<TransactionCapsule> transactions = blockCapsule.getTransactions();
          System.out.println(dateString + "," + transactions.size() / 3);
        }
      }
    } catch (IOException e) {
      logger.error("bwio write error", e);
    } catch (BadItemException e) {
      logger.error("BadItemException error", e);
    }
  }


  public static DB initDB(String path, Options dbOptions) throws IOException {
    File file = new File(path);
    DB database;
    try {
      database = factory.open(file, dbOptions);
    } catch (IOException e) {
      if (e.getMessage().contains("Corruption:")) {
        factory.repair(file, dbOptions);
        database = factory.open(file, dbOptions);
        logger.error("initDB IOException error", e);
      } else {
        throw e;
      }
    }
    return database;
  }
}

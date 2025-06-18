package org.tron.program;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.tron.common.utils.ByteArray;
import org.tron.core.Wallet;
import org.tron.core.capsule.AccountCapsule;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.protos.Protocol;
import org.tron.protos.contract.BalanceContract;

import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.tron.core.capsule.TransactionCapsule.validateSignature;
import static org.tron.program.DBConvert.newDefaultLevelDbOptions;
import static org.tron.program.PublicMethod.getAddressByteByPrivateKey;

@Slf4j(topic = "DB")
public class ScanDB {
  private static final String TO_ADDRESS = Wallet.getAddressPreFixString() + "abd4b9367799eaa3197fecb144eb71de1e049abc";
  public static void main(String[] args) throws IOException, InterruptedException {

    int txCount = 10_000; // 总交易数量改为100万笔
    int batchSize = 1000;  // 每批交易数量
//    int batchCount = txCount / batchSize; // 计算批次数量

    List<TransactionCapsule> transactions = new ArrayList<>(txCount);
    List<byte[]> ownerAddresses = new ArrayList<>(txCount);
    List<byte[]> hashes = new ArrayList<>(txCount);

    // 1. 串行构造交易
    for (int i = 0; i < txCount; i++) {
      String randomPrivateKey = PublicMethod.getRandomPrivateKey();
      byte[] privateKey = ByteArray.fromHexString(randomPrivateKey);
      byte[] ownerAddress = getAddressByteByPrivateKey(randomPrivateKey);

      BalanceContract.TransferContract transferContract = BalanceContract.TransferContract.newBuilder()
          .setAmount(1)
          .setOwnerAddress(ByteString.copyFrom(ownerAddress))
          .setToAddress(ByteString.copyFrom(ByteArray.fromHexString(TO_ADDRESS)))
          .build();

      TransactionCapsule txCapsule = new TransactionCapsule(
          transferContract, Protocol.Transaction.Contract.ContractType.TransferContract);
      txCapsule.sign(privateKey);

      transactions.add(txCapsule);
      ownerAddresses.add(ownerAddress);
      hashes.add(txCapsule.getTransactionId().getBytes());
    }

    Thread.sleep(3000);

    int threadCount = Runtime.getRuntime().availableProcessors();
    System.out.println("threadCount: " + threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(txCount);

    long s = System.nanoTime();
    for (int i = 0; i < txCount; i++) {
      final int index = i;
      executor.submit(() -> {
        try {
          validateSignature(
              ownerAddresses.get(index),
              transactions.get(index).getInstance(),
              hashes.get(index),
              null,
              null
          );

        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(); // 等待所有验签完成
    long e = System.nanoTime();
    System.out.println("耗时: " + (e - s) / 1000 + " μs");
    executor.shutdown();

  }


  public static void iterateDB(String sourcePath, BufferedWriter bw) throws IOException {
    Options options = newDefaultLevelDbOptions();
    DB db = initDB(sourcePath, options);

    try (DBIterator iterator = db.iterator()) {
      for (iterator.seekToLast(); iterator.hasPrev(); iterator.prev()) {
        byte[] value = iterator.peekPrev().getValue();
        AccountCapsule accountCapsule = new AccountCapsule(value);
        String accountName = accountCapsule.getAccountName().toStringUtf8();
        long latestConsumeTimeForEnergy = accountCapsule.getLatestConsumeTimeForEnergy();
        long energyUsage = accountCapsule.getEnergyUsage();
        long allFrozenBalanceForEnergy = accountCapsule.getAllFrozenBalanceForEnergy();

        try {
          bw.write(accountName + "," + allFrozenBalanceForEnergy + ","
              + energyUsage + "," + latestConsumeTimeForEnergy);
          bw.newLine();
        } catch (IOException e) {
          logger.error("bw write error", e);
        }
      }
    } catch (IOException e) {
      logger.error("bwio write error", e);
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
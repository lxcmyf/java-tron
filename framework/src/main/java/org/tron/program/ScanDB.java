package org.tron.program;

import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.tron.program.DBConvert.newDefaultLevelDbOptions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.tron.common.utils.StringUtil;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.exception.BadItemException;
import org.tron.protos.Protocol;
import org.tron.protos.contract.AssetIssueContractOuterClass;
import org.tron.protos.contract.BalanceContract;

@Slf4j(topic = "DB")
public class ScanDB {


  public static void main(String[] args) throws IOException {

    String sourcePath = args[0];
    iterateDB(sourcePath);
  }

  public static long txSize(TransactionCapsule trx) {
    return trx.getInstance().toBuilder().clearRet().build().getSerializedSize() +
        Constant.MAX_RESULT_SIZE_IN_TX;
  }

  public static void iterateDB(String sourcePath) throws IOException {
    Options options = newDefaultLevelDbOptions();
    DB db = initDB(sourcePath, options);
    try (DBIterator iterator = db.iterator()) {
      long maxNum = 0;
      for (iterator.seekToLast(); iterator.hasPrev(); iterator.prev()) {
        byte[] value = iterator.peekPrev().getValue();
        BlockCapsule blockCapsule = new BlockCapsule(value);
        long num = blockCapsule.getBlockId().getNum();
        maxNum = Math.max(maxNum, num);
        // 46257710
        // 55715000
        // 45391198
        if (num <= 75360000 || num >= 75440000) {
          break;
        }
        List<TransactionCapsule> transactions = blockCapsule.getTransactions();
        transactions.forEach(o -> {
          String txId = Hex.toHexString(o.getTransactionId().getBytes());
          Protocol.Transaction.Contract contract = o.getInstance().getRawData().getContract(0);
          Protocol.Transaction.Contract.ContractType type = contract.getType();
          String from = "";
          String to = "";
          switch (type) {
            case TransferContract:
              BalanceContract.TransferContract transferContract;
              try {
                transferContract = contract.getParameter().unpack(BalanceContract.TransferContract.class);
              } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage());
              }
              from = StringUtil.encode58Check(transferContract.getOwnerAddress().toByteArray());
              to = StringUtil.encode58Check(transferContract.getToAddress().toByteArray());
              break;
            case DelegateResourceContract:
              BalanceContract.DelegateResourceContract delegateResourceContract;
              try {
                delegateResourceContract = contract.getParameter().unpack(BalanceContract.DelegateResourceContract.class);
              } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage());
              }
              from = StringUtil.encode58Check(delegateResourceContract.getOwnerAddress().toByteArray());
              to = StringUtil.encode58Check(delegateResourceContract.getReceiverAddress().toByteArray());
              break;
            case UnDelegateResourceContract:
              BalanceContract.UnDelegateResourceContract unDelegateResourceContract;
              try {
                unDelegateResourceContract = contract.getParameter().unpack(BalanceContract.UnDelegateResourceContract.class);
              } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage());
              }
              from = StringUtil.encode58Check(unDelegateResourceContract.getOwnerAddress().toByteArray());
              to = StringUtil.encode58Check(unDelegateResourceContract.getReceiverAddress().toByteArray());
              break;
            default:
              break;
          }

          System.out.println(num + "," + txId + "," + type + "," + from + ","
              + to + "," + blockCapsule.getTimeStamp());
        });
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

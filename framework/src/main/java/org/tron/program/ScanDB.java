package org.tron.program;

import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.tron.program.DBConvert.newDefaultLevelDbOptions;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
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
import org.tron.protos.contract.AccountContract;
import org.tron.protos.contract.AssetIssueContractOuterClass;
import org.tron.protos.contract.BalanceContract;
import org.tron.protos.contract.SmartContractOuterClass;
import org.tron.protos.contract.WitnessContract;

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
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      for (iterator.seekToLast(); iterator.hasPrev(); iterator.prev()) {
        byte[] value = iterator.peekPrev().getValue();
        BlockCapsule blockCapsule = new BlockCapsule(value);
        long num = blockCapsule.getBlockId().getNum();
        maxNum = Math.max(maxNum, num);
        // 46257710
        // 55715000
        // 45391198
        if (num <= 69833743) {
          break;
        }
        if (num <= 69891327) {
          List<TransactionCapsule> transactions = blockCapsule.getTransactions();
          transactions.forEach(o -> {
            String txId = Hex.toHexString(o.getTransactionId().getBytes());
            Protocol.Transaction.raw rawData = o.getInstance().getRawData();
            Protocol.Transaction.Contract contract = rawData.getContract(0);
            long timestamp = rawData.getTimestamp();
            String dateString = sdf.format(new Date(timestamp));
            Protocol.Transaction.Contract.ContractType type = contract.getType();
            String from = null;
//          String to = null;
            switch (type) {
              case TransferContract:
                BalanceContract.TransferContract transferContract;
                try {
                  transferContract = contract.getParameter().unpack(BalanceContract.TransferContract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(transferContract.getOwnerAddress().toByteArray());
//              to = StringUtil.encode58Check(transferContract.getToAddress().toByteArray());
                break;
              case TriggerSmartContract:
                SmartContractOuterClass.TriggerSmartContract triggerSmartContract;
                try {
                  triggerSmartContract = contract.getParameter().unpack(SmartContractOuterClass.TriggerSmartContract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(triggerSmartContract.getOwnerAddress().toByteArray());
                break;
              case DelegateResourceContract:
                BalanceContract.DelegateResourceContract delegateResourceContract;
                try {
                  delegateResourceContract = contract.getParameter().unpack(BalanceContract.DelegateResourceContract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(delegateResourceContract.getOwnerAddress().toByteArray());
                break;
              case UnDelegateResourceContract:
                BalanceContract.UnDelegateResourceContract unDelegateResourceContract;
                try {
                  unDelegateResourceContract = contract.getParameter().unpack(BalanceContract.UnDelegateResourceContract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(unDelegateResourceContract.getOwnerAddress().toByteArray());
                break;
              case TransferAssetContract:
                AssetIssueContractOuterClass.TransferAssetContract transferAssetContract;
                try {
                  transferAssetContract = contract.getParameter().unpack(AssetIssueContractOuterClass.TransferAssetContract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(transferAssetContract.getOwnerAddress().toByteArray());
                break;
              case AccountCreateContract:
                AccountContract.AccountCreateContract accountCreateContract;
                try {
                  accountCreateContract = contract.getParameter().unpack(AccountContract.AccountCreateContract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(accountCreateContract.getOwnerAddress().toByteArray());
                break;
              case WithdrawBalanceContract:
                BalanceContract.WithdrawBalanceContract withdrawBalanceContract;
                try {
                  withdrawBalanceContract = contract.getParameter().unpack(BalanceContract.WithdrawBalanceContract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(withdrawBalanceContract.getOwnerAddress().toByteArray());
                break;
              case VoteWitnessContract:
                WitnessContract.VoteWitnessContract voteWitnessContract;
                try {
                  voteWitnessContract = contract.getParameter().unpack(WitnessContract.VoteWitnessContract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(voteWitnessContract.getOwnerAddress().toByteArray());
                break;
              case FreezeBalanceV2Contract:
                BalanceContract.FreezeBalanceV2Contract freezeBalanceV2Contract;
                try {
                  freezeBalanceV2Contract = contract.getParameter().unpack(BalanceContract.FreezeBalanceV2Contract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(freezeBalanceV2Contract.getOwnerAddress().toByteArray());
                break;
              case UnfreezeBalanceV2Contract:
                BalanceContract.UnfreezeBalanceV2Contract unfreezeBalanceV2Contract;
                try {
                  unfreezeBalanceV2Contract = contract.getParameter().unpack(BalanceContract.UnfreezeBalanceV2Contract.class);
                } catch (Exception ex) {
                  throw new RuntimeException(ex.getMessage());
                }
                from = StringUtil.encode58Check(unfreezeBalanceV2Contract.getOwnerAddress().toByteArray());
                break;
              default:
                break;
            }
            System.out.println(txId + "," + num + "," + type + "," + from + "," + dateString);
          });
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

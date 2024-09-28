package org.tron.core.net.messagehandler;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.crypto.ECKey;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.Wallet;
import org.tron.core.actuator.Actuator;
import org.tron.core.actuator.ActuatorFactory;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.config.args.Args;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.exception.P2pException;
import org.tron.core.exception.P2pException.TypeEnum;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.message.adv.TransactionMessage;
import org.tron.core.net.message.adv.TransactionsMessage;
import org.tron.core.net.peer.Item;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.service.adv.AdvService;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.Inventory.InventoryType;
import org.tron.protos.Protocol.ReasonCode;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.contract.BalanceContract;
import org.tron.protos.contract.SmartContractOuterClass;

import javax.annotation.PostConstruct;

import static org.tron.common.utils.StringUtil.encode58Check;

@Slf4j(topic = "net")
@Component
public class TransactionsMsgHandler implements TronMsgHandler {

  private static int MAX_TRX_SIZE = 50_000;
  private static int MAX_SMART_CONTRACT_SUBMIT_SIZE = 100;
  @Autowired
  private TronNetDelegate tronNetDelegate;
  @Autowired
  private AdvService advService;

  private BlockingQueue<TrxEvent> smartContractQueue = new LinkedBlockingQueue(MAX_TRX_SIZE);

  private BlockingQueue<Runnable> queue = new LinkedBlockingQueue();

  private int threadNum = Args.getInstance().getValidateSignThreadNum();
  private final String trxEsName = "trx-msg-handler";
  private ExecutorService trxHandlePool = ExecutorServiceManager.newThreadPoolExecutor(
      threadNum, threadNum, 0L,
      TimeUnit.MILLISECONDS, queue, trxEsName);
  private final String smartEsName = "contract-msg-handler";
  private final ScheduledExecutorService smartContractExecutor = ExecutorServiceManager
      .newSingleThreadScheduledExecutor(smartEsName);

  @Autowired
  private ChainBaseManager chainBaseManager;

  private ScheduledExecutorService pruneCheckpointThread = null;
  static TransactionMessage tx1 = null;
  static TransactionMessage tx2 = null;
  static TransactionMessage tx3 = null;
  static TransactionMessage tx4 = null;
  static TransactionMessage tx5 = null;

  public void init() {
    pruneCheckpointThread = ExecutorServiceManager.newSingleThreadScheduledExecutor("test-sunpump");
    pruneCheckpointThread.scheduleWithFixedDelay(() -> {
      try {
        generateTx();
      } catch (Throwable t) {
        logger.error("Exception in test-sunpump", t);
      }
    }, 12000, 30000, TimeUnit.MILLISECONDS);
    handleSmartContract();
  }

  public void close() {
    ExecutorServiceManager.shutdownAndAwaitTermination(trxHandlePool, trxEsName);
    ExecutorServiceManager.shutdownAndAwaitTermination(smartContractExecutor, smartEsName);
  }

  public boolean isBusy() {
    return queue.size() + smartContractQueue.size() > MAX_TRX_SIZE;
  }

  @Override
  public void processMessage(PeerConnection peer, TronMessage msg) throws P2pException {
    TransactionsMessage transactionsMessage = (TransactionsMessage) msg;
    check(peer, transactionsMessage);
    int smartContractQueueSize = 0;
    int trxHandlePoolQueueSize = 0;
    int dropSmartContractCount = 0;
    for (Transaction trx : transactionsMessage.getTransactions().getTransactionsList()) {
      int type = trx.getRawData().getContract(0).getType().getNumber();
      if (type == ContractType.TriggerSmartContract_VALUE
          || type == ContractType.CreateSmartContract_VALUE) {
        if (!smartContractQueue.offer(new TrxEvent(peer, new TransactionMessage(trx)))) {
          smartContractQueueSize = smartContractQueue.size();
          trxHandlePoolQueueSize = queue.size();
          dropSmartContractCount++;
        }
      } else {
        trxHandlePool.submit(() -> handleTransaction(peer, new TransactionMessage(trx)));
      }
    }

    if (dropSmartContractCount > 0) {
      logger.warn("Add smart contract failed, drop count: {}, queueSize {}:{}",
          dropSmartContractCount, smartContractQueueSize, trxHandlePoolQueueSize);
    }
  }

  private void check(PeerConnection peer, TransactionsMessage msg) throws P2pException {
    for (Transaction trx : msg.getTransactions().getTransactionsList()) {
      Item item = new Item(new TransactionMessage(trx).getMessageId(), InventoryType.TRX);
      if (!peer.getAdvInvRequest().containsKey(item)) {
        throw new P2pException(TypeEnum.BAD_MESSAGE,
            "trx: " + msg.getMessageId() + " without request.");
      }
      peer.getAdvInvRequest().remove(item);
    }
  }

  private void handleSmartContract() {
    smartContractExecutor.scheduleWithFixedDelay(() -> {
      try {
        while (queue.size() < MAX_SMART_CONTRACT_SUBMIT_SIZE && smartContractQueue.size() > 0) {
          TrxEvent event = smartContractQueue.take();
          trxHandlePool.submit(() -> handleTransaction(event.getPeer(), event.getMsg()));
        }
      } catch (InterruptedException e) {
        logger.warn("Handle smart server interrupted");
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Handle smart contract exception", e);
      }
    }, 1000, 20, TimeUnit.MILLISECONDS);
  }

  private void handleTransaction(PeerConnection peer, TransactionMessage trx) {
    if (peer.isBadPeer()) {
      logger.warn("Drop trx {} from {}, peer is bad peer", trx.getMessageId(),
          peer.getInetAddress());
      return;
    }

    if (advService.getMessage(new Item(trx.getMessageId(), InventoryType.TRX)) != null) {
      return;
    }



    Protocol.Transaction.Contract contract = trx.getTransactionCapsule().getInstance().getRawData().getContract(0);
    Protocol.Transaction.Contract.ContractType type = contract.getType();
    if (type == Protocol.Transaction.Contract.ContractType.TriggerSmartContract) {
      SmartContractOuterClass.TriggerSmartContract smartContract = null;
      try {
        smartContract = contract.getParameter().unpack(SmartContractOuterClass.TriggerSmartContract.class);

      } catch (InvalidProtocolBufferException e) {
        logger.error(e.getMessage());
      }
      if (smartContract != null) {
        String contractAddress = encode58Check(smartContract.getContractAddress().toByteArray());
        if ("TZFs5ch1R1C4mmjwrrmZqeqbUgGpxY1yWB".equals(contractAddress) &&
//        if (chainBaseManager.getDynamicPropertiesStore().getLatestBlockHeaderNumber() % 100 == 0 &&
            tx1 != null
//            &&
//            tx2 != null &&
//            tx3 != null &&
//            tx4 != null &&
//            tx5 != null
        ) {
          advService.fastBroadcastTransaction(tx1);
//          advService.fastBroadcastTransaction(tx2);
//          advService.fastBroadcastTransaction(tx3);
//          advService.fastBroadcastTransaction(tx4);
//          advService.fastBroadcastTransaction(tx5);
          tx1 = null;
//          tx2 = null;
//          tx3 = null;
//          tx4 = null;
//          tx5 = null;
          logger.info("sunpump test: txid: {}", trx.getTransactionCapsule().getTransactionId());
          logger.info("sunpump test: qiangpao tx1: {}", tx1.getTransactionCapsule().getTransactionId());
//          logger.info("sunpump test: qiangpao tx2: {}", tx2.getTransactionCapsule().getTransactionId());
//          logger.info("sunpump test: qiangpao tx3: {}", tx3.getTransactionCapsule().getTransactionId());
//          logger.info("sunpump test: qiangpao tx4: {}", tx4.getTransactionCapsule().getTransactionId());
//          logger.info("sunpump test: qiangpao tx5: {}", tx5.getTransactionCapsule().getTransactionId());
        }
      }
    }


    try {
      tronNetDelegate.pushTransaction(trx.getTransactionCapsule());
      advService.broadcast(trx);
    } catch (P2pException e) {
      logger.warn("Trx {} from peer {} process failed. type: {}, reason: {}",
          trx.getMessageId(), peer.getInetAddress(), e.getType(), e.getMessage());
      if (e.getType().equals(TypeEnum.BAD_TRX)) {
        peer.setBadPeer(true);
        peer.disconnect(ReasonCode.BAD_TX);
      }
    } catch (Exception e) {
      logger.error("Trx {} from peer {} process failed", trx.getMessageId(), peer.getInetAddress(),
          e);
    }
  }

  class TrxEvent {

    @Getter
    private PeerConnection peer;
    @Getter
    private TransactionMessage msg;
    @Getter
    private long time;

    public TrxEvent(PeerConnection peer, TransactionMessage msg) {
      this.peer = peer;
      this.msg = msg;
      this.time = System.currentTimeMillis();
    }
  }


  public Protocol.Transaction getSendcoin(byte[] to, long amount, byte[] owner, String priKey) throws ContractValidateException {
    Wallet.setAddressPreFixByte((byte) 0x41);
    ECKey temKey = null;
    try {
      BigInteger priK = new BigInteger(priKey, 16);
      temKey = ECKey.fromPrivate(priK);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    final ECKey ecKey = temKey;


    BalanceContract.TransferContract.Builder builder =
        BalanceContract.TransferContract.newBuilder();
    com.google.protobuf.ByteString bsTo = com.google.protobuf.ByteString.copyFrom(to);
    com.google.protobuf.ByteString bsOwner = ByteString.copyFrom(owner);
    builder.setToAddress(bsTo);
    builder.setOwnerAddress(bsOwner);
    builder.setAmount(amount);

    BalanceContract.TransferContract contract = builder.build();
    Protocol.Transaction transaction = createTransactionCapsule(contract, ContractType.TransferContract).getInstance();
    if (transaction == null || transaction.getRawData().getContractCount() == 0) {
      logger.error("sun pump generate tx faild");
    }
    transaction = signTransaction(ecKey, transaction);
    return transaction;
  }

  public static Protocol.Transaction signTransaction(ECKey ecKey,
                                                     Protocol.Transaction transaction) {
    if (ecKey == null || ecKey.getPrivKey() == null) {
      return null;
    }
    transaction = setTimestamp(transaction);
    return sign(transaction, ecKey);
  }

  public static Transaction sign(Transaction transaction, ECKey myKey) {
    Transaction.Builder transactionBuilderSigned = transaction.toBuilder();

    byte[] hash = Sha256Hash.hash(CommonParameter
        .getInstance().isECKeyCryptoEngine(), transaction.getRawData().toByteArray());
    List<Transaction.Contract> listContract = transaction.getRawData().getContractList();
    for (int i = 0; i < listContract.size(); i++) {
      ECKey.ECDSASignature signature = myKey.sign(hash);
      ByteString bsSign = ByteString.copyFrom(signature.toByteArray());
      transactionBuilderSigned.addSignature(
          bsSign);//Each contract may be signed with a different private key in the future.
    }

    transaction = transactionBuilderSigned.build();
    return transaction;
  }

  public static Transaction setTimestamp(Transaction transaction) {
    long currentTime = System.currentTimeMillis();//*1000000 + System.nanoTime()%1000000;
    Transaction.Builder builder = transaction.toBuilder();
    org.tron.protos.Protocol.Transaction.raw.Builder rowBuilder = transaction.getRawData()
        .toBuilder();
    rowBuilder.setTimestamp(currentTime);
    builder.setRawData(rowBuilder.build());
    return builder.build();
  }

  public static byte[] getFinalAddress(String priKey) {
    Wallet.setAddressPreFixByte((byte) 0x41);
    ECKey key = ECKey.fromPrivate(new BigInteger(priKey, 16));
    return key.getAddress();
  }

  public void generateTx() throws Exception {
    if (tx1 == null &&
        tx2 == null &&
        tx3 == null &&
        tx4 == null &&
        tx5 == null) {
      String sunPri = "";
      byte[] sunAddress = getFinalAddress(sunPri);

      String topri = "";
      byte[] tpAddress = getFinalAddress(topri);
      tx1 = new TransactionMessage(getSendcoin(tpAddress, 1, sunAddress, sunPri).toByteArray());
      logger.info("sunpump test: generate qiangpao tx1: {}", tx1.getTransactionCapsule().getTransactionId());
//      tx2 = new TransactionMessage(getSendcoin(tpAddress, 2, sunAddress, sunPri).toByteArray());
//      tx3 = new TransactionMessage(getSendcoin(tpAddress, 3, sunAddress, sunPri).toByteArray());
//      tx4 = new TransactionMessage(getSendcoin(tpAddress, 4, sunAddress, sunPri).toByteArray());
//      tx5 = new TransactionMessage(getSendcoin(tpAddress, 5, sunAddress, sunPri).toByteArray());
    }
  }

  public TransactionCapsule createTransactionCapsule(com.google.protobuf.Message message,
                                                     ContractType contractType) throws ContractValidateException {
    TransactionCapsule trx = new TransactionCapsule(message, contractType);
    trx.setTransactionCreate(true);
    if (contractType != ContractType.CreateSmartContract
        && contractType != ContractType.TriggerSmartContract) {
      List<Actuator> actList = ActuatorFactory.createActuator(trx, chainBaseManager);
      for (Actuator act : actList) {
        act.validate();
      }
    }
    trx.setTransactionCreate(false);
    if (contractType == ContractType.CreateSmartContract) {

      SmartContractOuterClass.CreateSmartContract contract = ContractCapsule
          .getSmartContractFromTransaction(trx.getInstance());
      long percent = contract.getNewContract().getConsumeUserResourcePercent();
      if (percent < 0 || percent > 100) {
        throw new ContractValidateException("percent must be >= 0 and <= 100");
      }
    }
    setTransaction(trx);
    return trx;
  }

  private void setTransaction(TransactionCapsule trx) {
    try {
      BlockCapsule.BlockId blockId = chainBaseManager.getHeadBlockId();
      if ("solid".equals(Args.getInstance().getTrxReferenceBlock())) {
        blockId = chainBaseManager.getSolidBlockId();
      }
      trx.setReference(blockId.getNum(), blockId.getBytes());
      long expiration = chainBaseManager.getHeadBlockTimeStamp() + Args.getInstance()
          .getTrxExpirationTimeInMilliseconds();
      trx.setExpiration(expiration);
      trx.setTimestamp();
    } catch (Exception e) {
      logger.error("Create transaction capsule failed.", e);
    }
  }
}
package org.tron.core.net.messagehandler;

import static org.mockito.ArgumentMatchers.any;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.tron.common.BaseTest;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.config.args.Args;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.adv.TransactionMessage;
import org.tron.core.net.message.adv.TransactionsMessage;
import org.tron.core.net.peer.Item;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.service.adv.AdvService;
import org.tron.protos.Protocol;
import org.tron.protos.contract.BalanceContract;

public class TransactionsMsgHandlerTest extends BaseTest {
  @BeforeClass
  public static void init() {
    Args.setParam(new String[]{"--output-directory", dbPath(), "--debug"},
        Constant.TEST_CONF);

  }

  @Test
  public void testProcessMessage() {
    TransactionsMsgHandler transactionsMsgHandler = new TransactionsMsgHandler();
    try {
      Assert.assertFalse(transactionsMsgHandler.isBusy());

      transactionsMsgHandler.init();

      PeerConnection peer = Mockito.mock(PeerConnection.class);
      TronNetDelegate tronNetDelegate = Mockito.mock(TronNetDelegate.class);
      chainBaseManager = Mockito.mock(ChainBaseManager.class);
      AdvService advService = Mockito.mock(AdvService.class);

      Field field = TransactionsMsgHandler.class.getDeclaredField("tronNetDelegate");
      field.setAccessible(true);
      field.set(transactionsMsgHandler, tronNetDelegate);

      Field field1 = TransactionsMsgHandler.class.getDeclaredField("chainBaseManager");
      field1.setAccessible(true);
      field1.set(transactionsMsgHandler, chainBaseManager);

      BalanceContract.TransferContract transferContract = BalanceContract.TransferContract
          .newBuilder()
          .setAmount(10)
          .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString("121212a9cf")))
          .setToAddress(ByteString.copyFrom(ByteArray.fromHexString("232323a9cf"))).build();

      long transactionTimestamp = DateTime.now().minusDays(4).getMillis();
      Protocol.Transaction trx = Protocol.Transaction.newBuilder().setRawData(
          Protocol.Transaction.raw.newBuilder().setTimestamp(transactionTimestamp)
          .setRefBlockNum(1)
          .setData(ByteString.copyFrom(("MIIEpAIBAAKCAQEAq/N2vWcvBFLZahk2/J/l+BrQ+UHT1vbP20901"
              + "lQPEi09D9ZFLzTrO+4aNFnXjN1P7UoI1I6F+7vN37hXW5i7/2YvX/N7sE6wJ+tY3wF1/r0fZbxpPy+6C"
              + "+3J7fh5/h3T+8jKrs6t  88/2J10+x/R7K/J3kZ9H0v3Yc/jB8lI11li7vhDjQ1Qu3IryN3y/fS714lK/"
              + "F6f2p36w6T14UV3v5zY45L7yp8eW0cJvb4l6q9WqsS6D8R7efp2g+4b7s/S+G8x/tF+qZc/15vK0P44/"
              + "pQIDAQABAoIBAQCOl6v99O9mNQ+Zd0H46i52+y6r8p/wBZz5oR+k3n1I7X276s+X55O9B9y2/383T2vOWP"
              + "/00W4B+Cv0o0DQOCW+h8b/62+883b7M+E54P4sQeQ6R8kf/Zb2sI6kb2Xxtw5H6L2/h3N6KQW6F/q6rIwE"
              + "43uMn8+X61X5wM00rQ5K2o5d2J/4lB/ZpUTsJBPs3ygJ2P6s").getBytes()))
          .addContract(
              Protocol.Transaction.Contract.newBuilder()
                  .setType(Protocol.Transaction.Contract.ContractType.TransferContract)
                  .setParameter(Any.pack(transferContract)).build()).build())
          .build();
      Map<Item, Long> advInvRequest = new ConcurrentHashMap<>();
      Item item = new Item(new TransactionMessage(trx).getMessageId(),
          Protocol.Inventory.InventoryType.TRX);
      advInvRequest.put(item, 0L);
      Mockito.when(peer.getAdvInvRequest()).thenReturn(advInvRequest);
      Mockito.when(chainBaseManager.contractCreateNewAccount(any())).thenReturn(true);

      CommonParameter.getInstance().setMaxCreateAccountTxSize(500);
      List<Protocol.Transaction> transactionList = new ArrayList<>();
      transactionList.add(trx);
      transactionsMsgHandler.processMessage(peer, new TransactionsMessage(transactionList));
      Assert.assertNull(advInvRequest.get(item));
      //Thread.sleep(10);
    } catch (Exception e) {
      Assert.fail();
    } finally {
      transactionsMsgHandler.close();
    }
  }
}

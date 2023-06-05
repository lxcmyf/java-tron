package org.tron.common.logsfilter;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.tron.common.logsfilter.trigger.BlockLogTrigger;

public class EventLoaderTest {

  @Test
  public void launchNativeQueue() {
    EventPluginConfig config = new EventPluginConfig();
    config.setSendQueueLength(1000);
    config.setBindPort(5555);
    config.setUseNativeQueue(true);

    List<TriggerConfig> triggerConfigList = new ArrayList<>();

    TriggerConfig blockTriggerConfig = new TriggerConfig();
    blockTriggerConfig.setTriggerName("block");
    blockTriggerConfig.setEnabled(true);
    blockTriggerConfig.setTopic("block");
    triggerConfigList.add(blockTriggerConfig);

    config.setTriggerConfigList(triggerConfigList);

    Assert.assertTrue(EventPluginLoader.getInstance().start(config));

    EventPluginLoader.getInstance().stopPlugin();
  }

  @Test
  public void testBlockLogTrigger() {
    BlockLogTrigger blt = new BlockLogTrigger();
    blt.setBlockHash(blt.getBlockHash());
    blt.setBlockNumber(blt.getBlockNumber());
    blt.setTransactionSize(blt.getTransactionSize());
    blt.setLatestSolidifiedBlockNumber(blt.getLatestSolidifiedBlockNumber());
    blt.setTriggerName(blt.getTriggerName());
    blt.setTimeStamp(blt.getTimeStamp());
    blt.setTransactionList(blt.getTransactionList());
    Assert.assertNotNull(blt.toString());

  }
}

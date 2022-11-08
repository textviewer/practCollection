package com.textviewer.group.rocketmq.simple;

import com.textviewer.group.rocketmq.common.Constant;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created by Enzo Cotter on 2022/11/9.
 */
public class RawPullConsumer {


    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");
        consumer.setNamespace(Constant.DEFAULT_NAMESRVADDR);
        Set<String> topics = new HashSet<>();
        topics.add(Constant.TOPIC);
        consumer.setRegisterTopics(topics);
        consumer.start();

        ExecutorService executors = Executors.newFixedThreadPool(topics.size(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "PullConsumerThread");
            }
        });

        for (String topic : consumer.getRegisterTopics()) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Set<MessageQueue> messageQueues = consumer.fetchMessageQueuesInBalance(topic);
                            if (messageQueues == null || messageQueues.isEmpty()) {
                                Thread.sleep(1000);
                                continue;
                            }
                            PullResult pullResult;
                            for (MessageQueue messageQueue : messageQueues) {
                                long offset = this.consumeFromOffset(messageQueue);
                                pullResult = consumer.pull(messageQueue, "*", offset, 32);
                                switch (pullResult.getPullStatus()) {
                                    case FOUND:
                                        List<MessageExt> msgs = pullResult.getMsgFoundList();

                                        if (msgs != null && !msgs.isEmpty()) {
                                            this.doSomething(msgs);
                                            System.out.println("finish!");
                                            // update offset to broker
                                            consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                                            // print pull tps
                                            this.incPullTPS(topic, pullResult.getMsgFoundList().size());
                                        }
                                        break;
                                    case OFFSET_ILLEGAL:
                                        consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                                        break;
                                    case NO_NEW_MSG:
                                        Thread.sleep(1);
                                        consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                                        break;
                                    case NO_MATCHED_MSG:
                                        consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                                        break;
                                        default:

                                }
                            }
                        } catch (MQClientException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (RemotingException e) {
                            e.printStackTrace();
                        } catch (MQBrokerException e) {
                            e.printStackTrace();
                        }
                    }
                }

                private void doSomething(List<MessageExt> msgs) {
                    msgs.forEach(System.out::println);
                }

                public long consumeFromOffset(MessageQueue messageQueue) throws MQClientException {
                    // -1 when started
                    long offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
                    if (offset < 0) {
                        // query from broker
                        offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
                    }
                    if (offset < 0) {
                        offset = consumer.maxOffset(messageQueue);
                    }
                    // make sure
                    if (offset < 0) {
                        offset = 0;
                    }
                    return offset;
                }

                public void incPullTPS(String topic, int pullSize) {
                    consumer.getDefaultMQPullConsumerImpl().getRebalanceImpl().getmQClientFactory()
                            .getConsumerStatsManager().incPullTPS(consumer.getConsumerGroup(), topic, pullSize);
                }
            });
        }
//        consumer.shutdown();
//        executors.shutdown();
    }
}

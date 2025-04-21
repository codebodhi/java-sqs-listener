package com.codebodhi.sqslistener;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;

public abstract class SqsListener {
  private static final Logger log = LoggerFactory.getLogger(SqsListener.class);
  private final String queueName;
  private final Duration pollingFrequency;
  private final Duration visibilityTimeout;
  private final int parallelism;
  private final SqsServiceClient sqsServiceClient;
  private final ArrayBlockingQueue<String> deleteMessageQueue;
  private final ThreadPoolExecutor erredMessagePool =
      new ThreadPoolExecutor(
          0,
          5,
          60,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(1000),
          new ThreadPoolExecutor.DiscardPolicy());

  private static class MsgReceiptHandle {
    String receiptHandle;
    boolean erred;
    int receivedCount;

    MsgReceiptHandle(String receiptHandle, boolean erred, int receivedCount) {
      this.receiptHandle = receiptHandle;
      this.erred = erred;
      this.receivedCount = receivedCount;
    }
  }

  public SqsListener(String queueName) {
    this(queueName, SqsListenerConfig.builder().build());
  }

  public SqsListener(String queueName, SqsListenerConfig sqsListenerConfig) {
    try {
      this.queueName = queueName;
      final DefaultConfig defaultConfig = DefaultConfig.INSTANCE;
      this.visibilityTimeout =
          (sqsListenerConfig.visibilityTimeout == null)
              ? defaultConfig.visibilityTimeout
              : sqsListenerConfig.visibilityTimeout;
      this.pollingFrequency =
          (sqsListenerConfig.pollingFrequency == null)
              ? defaultConfig.pollingFrequency
              : sqsListenerConfig.pollingFrequency;
      this.parallelism =
          (sqsListenerConfig.parallelism == 0)
              ? defaultConfig.parallelism
              : sqsListenerConfig.parallelism;
      this.sqsServiceClient =
          sqsListenerConfig.sqsClient != null
              ? (SqsServiceClient)
                  Class.forName(defaultConfig.sqsApiImplClass)
                      .getDeclaredConstructor(SqsClient.class)
                      .newInstance(sqsListenerConfig.sqsClient)
              : (SqsServiceClient)
                  Class.forName(defaultConfig.sqsApiImplClass)
                      .getDeclaredConstructor()
                      .newInstance();

      deleteMessageQueue = new ArrayBlockingQueue<>(defaultConfig.deleteMessageQueueSize);

      Executors.newSingleThreadScheduledExecutor()
          .scheduleAtFixedRate(
              () -> {
                try {
                  this.doProcess();
                } catch (Exception e) {
                  log.error("Error in doProcess()", e);
                }
              },
              0L,
              pollingFrequency.getSeconds(),
              TimeUnit.SECONDS);

      Executors.newSingleThreadScheduledExecutor()
          .scheduleAtFixedRate(
              () -> {
                try {
                  this.delete();
                } catch (Exception e) {
                  log.error("Error in delete()", e);
                }
              },
              pollingFrequency.getSeconds() / 2,
              pollingFrequency.getSeconds(),
              TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new SqsListenerException("Error during initialization", e);
    }
  }

  public abstract void process(String message) throws Exception;

  final void doProcess() {
    final int totalNoOfMessages = sqsServiceClient.getTotalNumberOfMessages(queueName);
    log.debug("Found a total of {} no. of messages", totalNoOfMessages);
    if (totalNoOfMessages == 0) {
      return;
    }

    final ExecutorService processingTaskPool =
        Executors.newFixedThreadPool(Math.min(parallelism, totalNoOfMessages));
    int processedMsgCount = 0;
    while (processedMsgCount < totalNoOfMessages) {
      final Set<SqsMessage> messages =
          sqsServiceClient.receiveMessage(
              queueName, pollingFrequency, parallelism, visibilityTimeout);

      if (messages.isEmpty()) {
        log.info("No messages received");
        return;
      }

      log.debug("Received {} messages", messages.size());
      final CompletionService<MsgReceiptHandle> processingTaskService =
          new ExecutorCompletionService<>(processingTaskPool);
      messages.forEach(
          message ->
              processingTaskService.submit(
                  () -> {
                    try {
                      process(message.body);
                    } catch (Exception e) {
                      log.error("Error processing message {}", message.body, e);
                      return new MsgReceiptHandle(
                          message.receiptHandle, true, message.receivedCount);
                    }
                    return new MsgReceiptHandle(
                        message.receiptHandle, false, message.receivedCount);
                  }));

      for (int i = 0; i < messages.size(); i++) {
        try {
          final Future<MsgReceiptHandle> future = processingTaskService.take();
          final MsgReceiptHandle msgReceiptHandle = future.get();
          if (!msgReceiptHandle.erred) {
            deleteMessageQueue.offer(msgReceiptHandle.receiptHandle);
          } else {
            erredMessagePool.submit(
                () ->
                    sqsServiceClient.changeVisibilityTimeout(
                        queueName,
                        msgReceiptHandle.receiptHandle,
                        visibilityTimeout.multipliedBy(msgReceiptHandle.receivedCount + 1)));
          }
          processedMsgCount++;
        } catch (Exception e) {
          throw new SqsListenerException("Error occurred processing message", e);
        }
      }
    }
    processingTaskPool.shutdown();
  }

  final void delete() {
    if (deleteMessageQueue.isEmpty()) {
      return;
    }
    log.debug("DeleteMessageQueue size = {} ", deleteMessageQueue.size());
    final int deleteTaskSize =
        deleteMessageQueue.size() > 10 ? (deleteMessageQueue.size() / 10 + 1) : 1;
    for (int i = 0; i < deleteTaskSize; i++) {
      final Set<String> toBeDeleted = new HashSet<>(10);
      deleteMessageQueue.drainTo(toBeDeleted, 10);
      log.debug("Messages toBeDeleted = {} ", toBeDeleted.size());
      final ExecutorService deleteTaskPool = Executors.newFixedThreadPool(deleteTaskSize);
      deleteTaskPool.submit(
          () -> sqsServiceClient.deleteMessages(queueName, new HashSet<>(toBeDeleted)));
    }
  }

  enum DefaultConfig {
    INSTANCE;
    final String sqsApiImplClass;
    final Duration visibilityTimeout;
    final Duration pollingFrequency;
    final int parallelism;
    final int deleteMessageQueueSize;

    DefaultConfig() {
      try (InputStream input =
          getClass().getClassLoader().getResourceAsStream("config.properties")) {
        final Properties properties = new Properties();
        properties.load(input);
        sqsApiImplClass = properties.getProperty("sqs-api-impl-class");
        visibilityTimeout =
            Duration.ofSeconds(Long.parseLong(properties.getProperty("visibility-timeout")));
        pollingFrequency =
            Duration.ofSeconds(Long.parseLong(properties.getProperty("polling-frequency")));
        parallelism = Integer.parseInt(properties.getProperty("parallelism"));
        deleteMessageQueueSize =
            Integer.parseInt(properties.getProperty("delete-message-queue-size"));
      } catch (IOException e) {
        throw new IllegalArgumentException("Error loading properties", e);
      }
    }
  }
}

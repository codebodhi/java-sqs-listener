package com.codebodhi.sqslistener;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.services.sqs.SqsClient;

public abstract class SqsListener {
  private final String queueName;
  private final Duration pollingFrequency;
  private final Duration visibilityTimeout;
  private final int parallelization;
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

  private static final Logger logger = Logger.getLogger(SqsListener.class.getName());

  static {
    System.setProperty("java.util.logging.config.file", "src/main/resources/logging.properties");
  }

  public SqsListener(String queueName) {
    this(queueName, SqsListenerConfig.builder().build());
  }

  public SqsListener(String queueName, SqsListenerConfig sqsListenerConfig) {
    try {
      this.queueName = queueName;
      final DefaultConfig defaultConfig = DefaultConfig.INSTANCE;
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
      this.visibilityTimeout =
          (sqsListenerConfig.visibilityTimeout == null)
              ? defaultConfig.visibilityTimeout
              : sqsListenerConfig.visibilityTimeout;
      this.pollingFrequency =
          (sqsListenerConfig.pollingFrequency == null)
              ? defaultConfig.pollingFrequency
              : sqsListenerConfig.pollingFrequency;
      this.parallelization =
          (sqsListenerConfig.parallelization == 0)
              ? defaultConfig.parallelization
              : sqsListenerConfig.parallelization;

      deleteMessageQueue = new ArrayBlockingQueue<>(defaultConfig.deleteMessageQueueSize);

      Executors.newScheduledThreadPool(1)
          .scheduleAtFixedRate(
              this::doProcess, 0L, pollingFrequency.getSeconds(), TimeUnit.SECONDS);

      Executors.newScheduledThreadPool(1)
          .scheduleAtFixedRate(
              this::delete,
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
    logger.info("Found a total of " + totalNoOfMessages + " no. of messages");
    if (totalNoOfMessages == 0) {
      return;
    }

    int processedMsgCount = 0;
    while (processedMsgCount < totalNoOfMessages) {
      final Set<SqsMessage> messages =
          sqsServiceClient.receiveMessage(
              queueName, pollingFrequency, parallelization, visibilityTimeout);
      logger.info("Received " + messages.size() + " messages");
      final ExecutorService processingTaskPool = Executors.newFixedThreadPool(parallelization);
      final CompletionService<MsgReceiptHandle> processingTaskService =
          new ExecutorCompletionService<>(processingTaskPool);
      messages.forEach(
          message ->
              processingTaskService.submit(
                  () -> {
                    try {
                      process(message.body);
                    } catch (Exception e) {
                      logger.log(Level.SEVERE, "Error processing", e);
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
      processingTaskPool.shutdown();
    }
  }

  final void delete() {
    logger.info("DeleteMessageQueue size = " + deleteMessageQueue.size());
    if (deleteMessageQueue.isEmpty()) {
      return;
    }
    final int deleteTaskSize =
        deleteMessageQueue.size() > 10 ? (deleteMessageQueue.size() / 10 + 1) : 1;
    for (int i = 0; i < deleteTaskSize; i++) {
      final Set<String> toBeDeleted = new HashSet<>(10);
      deleteMessageQueue.drainTo(toBeDeleted, 10);
      logger.info("Messages toBeDeleted = " + toBeDeleted.size());
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
    final int parallelization;
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
        parallelization = Integer.parseInt(properties.getProperty("parallelization"));
        deleteMessageQueueSize =
            Integer.parseInt(properties.getProperty("delete-message-queue-size"));
      } catch (IOException e) {
        throw new IllegalArgumentException("Error loading properties", e);
      }
    }
  }
}

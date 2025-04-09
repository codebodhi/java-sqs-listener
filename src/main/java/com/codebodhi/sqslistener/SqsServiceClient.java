package com.codebodhi.sqslistener;

import java.time.Duration;
import java.util.Set;

interface SqsServiceClient {
  String getQueueUrl(String queueName);

  int getTotalNumberOfMessages(String queueName);

  Set<SqsMessage> receiveMessage(
      String queueName, Duration pollingFrequency, int parallelization, Duration visibilityTimeout);

  void deleteMessages(String queueName, Set<String> msgReceiptHandle);

  void changeVisibilityTimeout(String queueName, String msgReceiptHandle, Duration duration);
}

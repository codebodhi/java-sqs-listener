package com.codebodhi.sqslistener;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

class SqsServiceClientSdk2 implements SqsServiceClient {
  private final SqsClient sqsClient;

  SqsServiceClientSdk2() {
    sqsClient =
        SqsClient.builder()
            .region(DefaultAwsRegionProviderChain.builder().build().getRegion())
            .build();
  }

  SqsServiceClientSdk2(SqsClient sqsClient) {
    this.sqsClient = sqsClient;
  }

  @Override
  public String getQueueUrl(String queueName) {
    return sqsClient
        .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build())
        .queueUrl();
  }

  @Override
  public int getTotalNumberOfMessages(String queueName) {
    final Map<QueueAttributeName, String> attributes =
        sqsClient
            .getQueueAttributes(
                GetQueueAttributesRequest.builder()
                    .queueUrl(getQueueUrl(queueName))
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                    .build())
            .attributes();
    return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
  }

  @Override
  public Set<SqsMessage> receiveMessage(
      String queueName,
      Duration pollingFrequency,
      int parallelization,
      Duration visibilityTimeout) {
    final int waitTimeout = (int) Math.min(pollingFrequency.toSeconds(), 20);

    final int maxNumberOfMessages = Math.min(parallelization, 10);

    final ReceiveMessageResponse response =
        sqsClient.receiveMessage(
            ReceiveMessageRequest.builder()
                .queueUrl(getQueueUrl(queueName))
                .waitTimeSeconds(waitTimeout)
                .maxNumberOfMessages(maxNumberOfMessages)
                .visibilityTimeout((int) visibilityTimeout.getSeconds())
                .messageSystemAttributeNames(
                    MessageSystemAttributeName.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP,
                    MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
                .build());

    return response.messages().stream()
        .map(
            message ->
                new SqsMessage(
                    message.messageId(),
                    message.receiptHandle(),
                    message.body(),
                    Instant.ofEpochMilli(
                        Long.parseLong(
                            message
                                .attributes()
                                .get(
                                    MessageSystemAttributeName
                                        .APPROXIMATE_FIRST_RECEIVE_TIMESTAMP))),
                    Integer.valueOf(
                        message
                            .attributes()
                            .get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT))))
        .collect(Collectors.toSet());
  }

  @Override
  public void deleteMessages(String queueName, Set<String> msgReceiptHandles) {
    if (msgReceiptHandles.size() > 10) {
      throw new IllegalArgumentException("Delete batch size can't be greater than 10");
    }
    sqsClient.deleteMessageBatch(
        builder ->
            builder
                .queueUrl(getQueueUrl(queueName))
                .entries(
                    msgReceiptHandles.stream()
                        .map(
                            receiptHandle ->
                                DeleteMessageBatchRequestEntry.builder()
                                    .receiptHandle(receiptHandle)
                                    .id(UUID.randomUUID().toString())
                                    .build())
                        .collect(Collectors.toSet()))
                .build());
  }

  @Override
  public void changeVisibilityTimeout(
      String queueName, String msgReceiptHandle, Duration duration) {
    sqsClient.changeMessageVisibility(
        ChangeMessageVisibilityRequest.builder()
            .queueUrl(getQueueUrl(queueName))
            .receiptHandle(msgReceiptHandle)
            .visibilityTimeout((int) duration.getSeconds())
            .build());
  }
}

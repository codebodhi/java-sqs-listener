package com.codebodhi.sqslistener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

class SqsServiceClientSdk2Test {
  SqsServiceClientSdk2 sqsApiSdk2;
  @Mock SqsAsyncClient sqsClient;

  @BeforeEach
  void before() {
    MockitoAnnotations.openMocks(this);
    sqsApiSdk2 = new SqsServiceClientSdk2(sqsClient);
  }

  @Test
  void shouldGetSqsUrl() throws ExecutionException, InterruptedException {
    String queueName = "test-queue";
    String queueUrl = "dummy://mock-queue";
    when(sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GetQueueUrlResponse.builder().queueUrl(queueUrl).build()));
    assertEquals(queueUrl, sqsApiSdk2.getQueueUrl(queueName));
  }

  @Test
  void shouldGetTotalNoOfMessages() {
    String queueName = "test-queue";
    String queueUrl = "dummy://mock-queue";
    when(sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GetQueueUrlResponse.builder().queueUrl(queueUrl).build()));

    Map<QueueAttributeName, String> attributeMap = new HashMap<>();
    attributeMap.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "10");

    when(sqsClient.getQueueAttributes(
            GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GetQueueAttributesResponse.builder().attributes(attributeMap).build()));
    assertEquals(10, sqsApiSdk2.getTotalNumberOfMessages(queueName));
  }

  @Test
  void shouldReceiveMessagesPerConfig() {
    String queueName = "test-queue";
    String queueUrl = "dummy://mock-queue";

    int parallelization = 3;
    int pollingFrequencyInSecs = 1;
    int visibilityTimeoutInSecs = 30;

    when(sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                GetQueueUrlResponse.builder().queueUrl(queueUrl).build()));

    when(sqsClient.receiveMessage(
            ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(visibilityTimeoutInSecs)
                .waitTimeSeconds(pollingFrequencyInSecs)
                .maxNumberOfMessages(parallelization)
                .messageSystemAttributeNames(
                    MessageSystemAttributeName.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP,
                    MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
                .build()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ReceiveMessageResponse.builder()
                    .messages(
                        Message.builder()
                            .messageId("msg1")
                            .body("msg1-body")
                            .attributes(messagAttributeMap(1))
                            .build(),
                        Message.builder()
                            .messageId("msg2")
                            .body("msg2-body")
                            .attributes(messagAttributeMap(2))
                            .build(),
                        Message.builder()
                            .messageId("msg3")
                            .body("msg3-body")
                            .attributes(messagAttributeMap(3))
                            .build())
                    .build()));

    Set<SqsMessage> receivedMessages =
        sqsApiSdk2.receiveMessage(
            queueName,
            Duration.ofSeconds(pollingFrequencyInSecs),
            parallelization,
            Duration.ofSeconds(visibilityTimeoutInSecs));

    assertEquals(3, receivedMessages.size());

    List<SqsMessage> sortedMessages =
        receivedMessages.stream()
            .sorted(Comparator.comparing(m -> m.messageId))
            .collect(Collectors.toList());

    assertEquals("msg1-body", sortedMessages.get(0).body);
    assertEquals("msg2-body", sortedMessages.get(1).body);
    assertEquals("msg3-body", sortedMessages.get(2).body);
  }

  static Map<MessageSystemAttributeName, String> messagAttributeMap(int msgReceiveCount) {
    Map<MessageSystemAttributeName, String> attributeMap = new HashMap<>();
    attributeMap.put(
        MessageSystemAttributeName.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP,
        String.valueOf(Instant.now().getEpochSecond()));
    attributeMap.put(
        MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, String.valueOf(msgReceiveCount));
    return attributeMap;
  }
}

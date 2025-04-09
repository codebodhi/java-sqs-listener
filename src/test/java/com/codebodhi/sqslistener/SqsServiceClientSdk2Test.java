package com.codebodhi.sqslistener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

class SqsServiceClientSdk2Test {
  SqsServiceClientSdk2 sqsApiSdk2;
  @Mock SqsClient sqsClient;

  @BeforeEach
  void before() {
    MockitoAnnotations.openMocks(this);
    sqsApiSdk2 = new SqsServiceClientSdk2(sqsClient);
  }

  @Test
  void shouldGetSqsUrl() {
    String queueName = "test-queue";
    String queueUrl = "dummy://mock-queue";
    when(sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
        .thenReturn(GetQueueUrlResponse.builder().queueUrl(queueUrl).build());
    assertEquals(queueUrl, sqsApiSdk2.getQueueUrl(queueName));
  }

  @Test
  void shouldGetTotalNoOfMessages() {
    String queueName = "test-queue";
    String queueUrl = "dummy://mock-queue";
    when(sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
        .thenReturn(GetQueueUrlResponse.builder().queueUrl(queueUrl).build());
    when(sqsClient.getQueueAttributes(
            GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                .build()))
        .thenReturn(
            GetQueueAttributesResponse.builder()
                .attributes(Map.of(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "10"))
                .build());
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
        .thenReturn(GetQueueUrlResponse.builder().queueUrl(queueUrl).build());

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
            ReceiveMessageResponse.builder()
                .messages(
                    Message.builder()
                        .messageId("msg1")
                        .body("msg1-body")
                        .attributes(
                            Map.of(
                                MessageSystemAttributeName.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP,
                                String.valueOf(Instant.now().getEpochSecond()),
                                MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT,
                                "1"))
                        .build(),
                    Message.builder()
                        .messageId("msg2")
                        .body("msg2-body")
                        .attributes(
                            Map.of(
                                MessageSystemAttributeName.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP,
                                String.valueOf(Instant.now().getEpochSecond()),
                                MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT,
                                "2"))
                        .build(),
                    Message.builder()
                        .messageId("msg3")
                        .body("msg3-body")
                        .attributes(
                            Map.of(
                                MessageSystemAttributeName.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP,
                                String.valueOf(Instant.now().getEpochSecond()),
                                MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT,
                                "3"))
                        .build())
                .build());

    Set<SqsMessage> receivedMessages =
        sqsApiSdk2.receiveMessage(
            queueName,
            Duration.ofSeconds(pollingFrequencyInSecs),
            parallelization,
            Duration.ofSeconds(visibilityTimeoutInSecs));

    assertEquals(3, receivedMessages.size());

    List<SqsMessage> sortedMessages =
        receivedMessages.stream().sorted(Comparator.comparing(m -> m.messageId)).toList();

    assertEquals("msg1-body", sortedMessages.get(0).body);
    assertEquals("msg2-body", sortedMessages.get(1).body);
    assertEquals("msg3-body", sortedMessages.get(2).body);
  }
}

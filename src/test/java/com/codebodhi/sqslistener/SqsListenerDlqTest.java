package com.codebodhi.sqslistener;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

class SqsListenerDlqTest {
  static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:latest");
  static final LocalStackContainer localstack =
      new LocalStackContainer(LOCALSTACK_IMAGE).withServices(LocalStackContainer.Service.SQS);
  static SqsClient sqsClient;
  static final String queueName = "test-queue";
  static String queueUrl;
  static final String deadLetterQueueName = "test-queue-error";
  static String deadLetterQueueUrl;

  @BeforeAll
  static void setup() {
    localstack.start();
    sqsClient =
        SqsClient.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
            .region(Region.of(localstack.getRegion()))
            .build();

    // Create DLQ
    CreateQueueResponse dlqResponse =
        sqsClient.createQueue(CreateQueueRequest.builder().queueName(deadLetterQueueName).build());

    deadLetterQueueUrl = dlqResponse.queueUrl();
    // Get DLQ ARN
    String dlqArn =
        sqsClient
            .getQueueAttributes(
                GetQueueAttributesRequest.builder()
                    .queueUrl(deadLetterQueueUrl)
                    .attributeNames(QueueAttributeName.QUEUE_ARN)
                    .build())
            .attributes()
            .get(QueueAttributeName.QUEUE_ARN);

    CreateQueueResponse createQueueResponse =
        sqsClient.createQueue(
            builder ->
                builder
                    .queueName(queueName)
                    .attributes(
                        Map.of(
                            QueueAttributeName.REDRIVE_POLICY,
                            String.format(
                                "{\"maxReceiveCount\":\"3\", \"deadLetterTargetArn\":\"%s\"}",
                                dlqArn)))
                    .build());
    Assertions.assertNotNull(createQueueResponse, "createQueue failed");
    Assertions.assertNotNull(createQueueResponse.queueUrl(), "could not get queueUrl");
    queueUrl = createQueueResponse.queueUrl();
  }

  @Test
  void shouldRouteToDeadLetterQueue() {
    final String actualMessageBody = "lorem ipsum";
    sqsClient.sendMessage(
        builder -> builder.queueUrl(queueUrl).messageBody(actualMessageBody).build());
    new SqsListener(
        queueName,
        SqsListenerConfig.builder()
            .pollingFrequency(Duration.ofSeconds(3))
            .visibilityTimeout(Duration.ofSeconds(3))
            .sqsClient(sqsClient)
            .build()) {
      @Override
      public void process(String message) {
        throw new RuntimeException("Error in client processing");
      }
    };

    await()
        .atMost(1, TimeUnit.MINUTES)
        .until(
            () ->
                sqsClient
                    .receiveMessage(
                        builder ->
                            builder
                                .queueUrl(deadLetterQueueUrl)
                                .waitTimeSeconds(1)
                                .maxNumberOfMessages(10))
                    .hasMessages());
  }
}

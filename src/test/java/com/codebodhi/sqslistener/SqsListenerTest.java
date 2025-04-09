package com.codebodhi.sqslistener;

import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.*;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;

class SqsListenerTest {
  static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:latest");
  static final LocalStackContainer localstack =
      new LocalStackContainer(LOCALSTACK_IMAGE).withServices(LocalStackContainer.Service.SQS);
  static PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:15.1")
          .withDatabaseName("testdb")
          .withUsername("testuser")
          .withPassword("testpass");
  static SqsClient sqsClient;
  static final String queueName = "test-queue";
  static String queueUrl;
  static Connection conn;

  @BeforeAll
  static void setup() throws SQLException {
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
    CreateQueueResponse createQueueResponse =
        sqsClient.createQueue(builder -> builder.queueName(queueName).build());
    Assertions.assertNotNull(createQueueResponse, "createQueue failed");
    Assertions.assertNotNull(createQueueResponse.queueUrl(), "could not get queueUrl");
    queueUrl = createQueueResponse.queueUrl();

    postgres.start();
    initMockDatabase();
    conn =
        DriverManager.getConnection(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  @AfterAll
  static void teardown() {
    localstack.stop();
    postgres.stop();
  }

  @Test
  void shouldProcessAMessage() {
    final String actualMessageBody = "lorem ipsum";
    sqsClient.sendMessage(
        builder -> builder.queueUrl(queueUrl).messageBody(actualMessageBody).build());
    final String[] expectedMessageBody = new String[1];
    new SqsListener(queueName, SqsListenerConfig.builder().sqsClient(sqsClient).build()) {
      @Override
      public void process(String message) {
        expectedMessageBody[0] = message;
      }
    };

    await()
        .atMost(1, TimeUnit.MINUTES)
        .until(
            () -> {
              boolean messageReceived = Objects.equals(expectedMessageBody[0], actualMessageBody);
              boolean hasNoMoreMessages =
                  !sqsClient
                      .receiveMessage(
                          builder ->
                              builder.queueUrl(queueUrl).waitTimeSeconds(1).maxNumberOfMessages(10))
                      .hasMessages();
              return messageReceived && hasNoMoreMessages;
            });
  }

  @Test
  void shouldProcessMultipleMessages() {
    int messageSize = 1000;
    for (int i = 1; i <= messageSize; i++) {
      int iteration = i;
      sqsClient.sendMessage(
          builder ->
              builder
                  .queueUrl(queueUrl)
                  .messageBody(
                      "{\n"
                          + "  \"id\": "
                          + iteration
                          + ",\n"
                          + "  \"name\": \"test-user-"
                          + iteration
                          + "  \"\n"
                          + "}")
                  .build());
    }

    final ObjectMapper mapper = new ObjectMapper();
    new SqsListener(
        queueName,
        SqsListenerConfig.builder()
            .parallelization(5)
            .pollingFrequency(Duration.ofSeconds(1))
            .sqsClient(sqsClient)
            .build()) {
      @Override
      public void process(String message) {
        try {
          final JsonNode node = mapper.readTree(message);
          final int id = node.at("/id").asInt();
          final String name = node.at("/name").asText();
          insertData(id, name);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };

    await()
        .pollInterval(Duration.ofSeconds(1))
        .atMost(5, TimeUnit.MINUTES)
        .until(
            () ->
                !sqsClient
                        .receiveMessage(
                            builder ->
                                builder
                                    .queueUrl(queueUrl)
                                    .waitTimeSeconds(1)
                                    .maxNumberOfMessages(10))
                        .hasMessages()
                    && getTotalCount() == messageSize);
  }

  static void initMockDatabase() {
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  void insertData(int id, String name) {
    String insertSQL = "INSERT INTO users (id, name) VALUES (?, ?)";
    try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
      System.out.println("Inserting id = " + id + " name  " + name);
      stmt.setInt(1, id);
      stmt.setString(2, name);
      int rowsUpdated = stmt.executeUpdate();
      System.out.println(rowsUpdated + " record(s) updated.");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  int getTotalCount() {
    int count = 0;
    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("select count(*) from users");
      if (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    System.out.println("total count = " + count);
    return count;
  }
}

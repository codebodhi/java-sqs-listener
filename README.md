# Java SQS Listener 
[![Maven Central](https://img.shields.io/maven-central/v/com.codebodhi/java-sqs-listener.svg?label=Maven%20Central)](https://search.maven.org/artifact/com.codebodhi/java-sqs-listener)

Designed for simplicity and performance, this library allows you to continuously poll messages from an SQS queue with configurable frequency and parallel execution. It’s built solely on AWS SDK for Java 2.x—no additional dependencies required.

## 🚀 Features

- **Framework-Agnostic Integration**: Easily integrates with any Java application and works with any dependency injection (DI) framework—no need for external libraries like Spring.
- **Minimal Setup**: Only the SQS queue name is needed to start polling with the simplest configuration.
- **Customizable**: Configure concurrent message processing with a single parameter. Polling frequency and visibility timeout are fully adjustable. Uses a built-in SqsClient by default, or you can supply your own via configuration.
- **Auto-Delete**: Successfully processed messages are automatically batched and deleted from the queue.
- **Error Handling**: Failed messages are delayed and retried until the maximum number of attempts is reached.
- **Designed for Extensibility**: Built on AWS SDK for Java 2.x, with a flexible architecture that makes upgrading to future SDK versions straightforward.
- **End-to-End Testing with Testcontainers**: All aspects of the library, including integrations with AWS services, are validated through Testcontainers to ensure reliability before release.
- **Real-World Tested**: The library has been thoroughly tested in a Spring Boot application running on AWS EC2, efficiently processing tens of thousands of messages.

## 🛠 Installation

#### Maven
Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.codebodhi</groupId>
    <artifactId>java-sqs-listener</artifactId>
    <version>2.7.0</version>
</dependency>
```

#### Gradle
Add the following dependency to your `build.gradle`:
```
  implementation 'com.codebodhi:java-sqs-listener:2.7.0'
```

## 🔧 Usage

### Plain Java
➤ SQS Name
````Java
import com.codebodhi.sqslistener.SqsListener;

public class MySqsListener {
    public static void main(String[] args) {
        String queueName = "my-queue";
        new SqsListener(queueName) {
            @Override
            public void process(String message) {
                //process the message
            }
        };
    }
}
````

➤ SQS name & configuration options
````Java
import com.codebodhi.sqslistener.SqsListener;
import com.codebodhi.sqslistener.SqsListenerConfig;
import java.time.Duration;

public class MySqsListener {
    public static void main(String[] args) {
        String queueName = "my-queue";
        new SqsListener(queueName,
                SqsListenerConfig.builder()
                        .parallelism(5)
                        .pollingFrequency(Duration.ofSeconds(5))
                        .visibilityTimeout(Duration.ofSeconds(60))
                        .build()) {
            @Override
            public void process(String message) {
                //process the message
            }
        };
    }
}
````

### Usage within a DI container like Spring 
➤ Register SqsListenerConfig as a Spring @Bean in your configuration class 
````Java
import com.codebodhi.sqslistener.SqsListenerConfig;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SqsListenerConfiguration {
    @Bean("mySqsListenerConfig")
    public SqsListenerConfig mySqsListenerConfig() {
        return SqsListenerConfig.builder()
                .parallelism(5)
                .pollingFrequency(Duration.ofSeconds(5))
                .visibilityTimeout(Duration.ofSeconds(60))
                .build();
    }
}
````
➤ Create a Spring-managed service class that extends SqsListener 
````Java
import com.codebodhi.sqslistener.SqsListener;
import com.codebodhi.sqslistener.SqsListenerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MySqsListener extends SqsListener {
    public MySqsListener(
            @Value("${my-queue}") String queueName,
            @Qualifier("mySqsListenerConfig") SqsListenerConfig sqsListenerConfig) {
        // super constructor
        super(queueName, sqsListenerConfig);
        // any other needed initialization here
    }

    @Override
    public void process(String message) {
        // process the message
    }
}
````
## 🧠 How It Works

Refer to this [java-sqs-listener-springboot-example](https://github.com/codebodhi/java-sqs-listener-springboot-example) for a comprehensive example demonstrating the integration of the library within a Spring Boot application. 
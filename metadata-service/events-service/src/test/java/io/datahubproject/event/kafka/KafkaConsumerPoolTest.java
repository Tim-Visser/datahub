package io.datahubproject.event.kafka;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.ConsumerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaConsumerPoolTest {

  @Mock private ConsumerFactory<String, GenericRecord> consumerFactory;

  @Mock private KafkaConsumer<String, GenericRecord> kafkaConsumer;

  private KafkaConsumerPool kafkaConsumerPool;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.initMocks(this); // Initialize mocks
    when(consumerFactory.createConsumer()).thenReturn(kafkaConsumer);
    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(kafkaConsumer.partitionsFor(anyString(), any(Duration.class)))
        .thenReturn(Collections.emptyList());

    kafkaConsumerPool = new KafkaConsumerPool(consumerFactory, 2, 5, Duration.ofSeconds(2), null);
  }

  @Test
  public void testPoolInitialization() {
    // Verify the consumer is created the initial number of times
    verify(consumerFactory, times(2)).createConsumer();
  }

  @Test
  public void testBorrowConsumerWhenAvailable() throws InterruptedException {
    // Setup initial state
    KafkaConsumer<String, GenericRecord> consumer =
        kafkaConsumerPool.borrowConsumer(1000, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    // Assertions
    assertNotNull(consumer, "Consumer should not be null when borrowed");
    verify(consumerFactory, times(2)).createConsumer(); // Initial + this borrow
    kafkaConsumerPool.returnConsumer(consumer);
  }

  @Test
  public void testBorrowConsumerReturnsNullAfterTimeout() throws InterruptedException {
    // First, exhaust the pool by borrowing all initial consumers
    KafkaConsumer<String, GenericRecord> kafkaConsumer1 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    KafkaConsumer<String, GenericRecord> kafkaConsumer2 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    KafkaConsumer<String, GenericRecord> kafkaConsumer3 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    KafkaConsumer<String, GenericRecord> kafkaConsumer4 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    KafkaConsumer<String, GenericRecord> kafkaConsumer5 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    // Now pool is empty and at max size, borrowing should timeout and return null
    long startTime = System.currentTimeMillis();
    KafkaConsumer<String, GenericRecord> consumer =
        kafkaConsumerPool.borrowConsumer(500, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    long elapsedTime = System.currentTimeMillis() - startTime;

    assertNull(consumer, "Consumer should be null after timeout when pool is exhausted");
    assertTrue(elapsedTime >= 500, "Should wait at least the timeout duration");
    assertTrue(elapsedTime < 1000, "Should not wait significantly longer than timeout");
    kafkaConsumerPool.returnConsumer(kafkaConsumer1);
    kafkaConsumerPool.returnConsumer(kafkaConsumer2);
    kafkaConsumerPool.returnConsumer(kafkaConsumer3);
    kafkaConsumerPool.returnConsumer(kafkaConsumer4);
    kafkaConsumerPool.returnConsumer(kafkaConsumer5);
  }

  @Test
  public void testBorrowConsumerWithZeroTimeout() throws InterruptedException {
    // Exhaust the pool
    KafkaConsumer<String, GenericRecord> kafkaConsumer1 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    KafkaConsumer<String, GenericRecord> kafkaConsumer2 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    KafkaConsumer<String, GenericRecord> kafkaConsumer3 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    KafkaConsumer<String, GenericRecord> kafkaConsumer4 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    KafkaConsumer<String, GenericRecord> kafkaConsumer5 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    // Try to borrow with 0 timeout - should return null immediately
    long startTime = System.currentTimeMillis();
    KafkaConsumer<String, GenericRecord> consumer =
        kafkaConsumerPool.borrowConsumer(0, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    long elapsedTime = System.currentTimeMillis() - startTime;

    assertNull(consumer, "Consumer should be null immediately with 0 timeout");
    assertTrue(elapsedTime < 100, "Should return almost immediately");
    kafkaConsumerPool.returnConsumer(kafkaConsumer1);
    kafkaConsumerPool.returnConsumer(kafkaConsumer2);
    kafkaConsumerPool.returnConsumer(kafkaConsumer3);
    kafkaConsumerPool.returnConsumer(kafkaConsumer4);
    kafkaConsumerPool.returnConsumer(kafkaConsumer5);
  }

  @Test
  public void testBorrowConsumerSucceedsWhenConsumerReturnedDuringWait()
      throws InterruptedException {
    KafkaConsumerPool limitedPool =
        new KafkaConsumerPool(consumerFactory, 1, 1, Duration.ofSeconds(2), null);

    // Borrow the only consumer
    KafkaConsumer<String, GenericRecord> consumer1 =
        limitedPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    assertNotNull(consumer1);

    // Create a thread that will return the consumer after a short delay
    Thread returnThread =
        new Thread(
            () -> {
              try {
                Thread.sleep(200); // Wait 200ms before returning
                limitedPool.returnConsumer(consumer1);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    returnThread.start();

    // Try to borrow - should wait and succeed when consumer is returned
    long startTime = System.currentTimeMillis();
    KafkaConsumer<String, GenericRecord> consumer2 =
        limitedPool.borrowConsumer(1000, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    long elapsedTime = System.currentTimeMillis() - startTime;

    assertNotNull(consumer2, "Should receive consumer when it's returned during wait");
    assertSame(consumer2, consumer1, "Should receive the same consumer that was returned");
    assertTrue(elapsedTime >= 200, "Should wait until consumer is returned");
    assertTrue(elapsedTime < 1000, "Should not wait for full timeout");

    returnThread.join();
    limitedPool.shutdownPool();
  }

  @Test
  public void testBorrowConsumerReturnsNullWhenConsumerNotReturnedInTime()
      throws InterruptedException {
    KafkaConsumerPool limitedPool =
        new KafkaConsumerPool(consumerFactory, 1, 1, Duration.ofSeconds(2), null);

    // Borrow the only consumer
    KafkaConsumer<String, GenericRecord> consumer1 =
        limitedPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    assertNotNull(consumer1);

    // Create a thread that will return the consumer after timeout expires
    Thread returnThread =
        new Thread(
            () -> {
              try {
                Thread.sleep(800); // Wait longer than the timeout
                limitedPool.returnConsumer(consumer1);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    returnThread.start();

    // Try to borrow with shorter timeout - should return null
    long startTime = System.currentTimeMillis();
    KafkaConsumer<String, GenericRecord> consumer2 =
        limitedPool.borrowConsumer(300, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    long elapsedTime = System.currentTimeMillis() - startTime;

    assertNull(consumer2, "Should return null when consumer not returned within timeout");
    assertTrue(elapsedTime >= 300, "Should wait for the full timeout");
    assertTrue(elapsedTime < 600, "Should not wait significantly longer");

    returnThread.join();
    limitedPool.shutdownPool();
  }

  @Test
  public void testMultipleConcurrentBorrowsWithTimeout() throws InterruptedException {
    KafkaConsumerPool limitedPool =
        new KafkaConsumerPool(consumerFactory, 2, 2, Duration.ofSeconds(2), null);

    limitedPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    limitedPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    // Try to borrow from multiple threads
    Thread thread1 =
        new Thread(
            () -> {
              try {
                KafkaConsumer<String, GenericRecord> consumer =
                    limitedPool.borrowConsumer(300, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
                assertNull(consumer, "Thread 1 should timeout");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    Thread thread2 =
        new Thread(
            () -> {
              try {
                KafkaConsumer<String, GenericRecord> consumer =
                    limitedPool.borrowConsumer(300, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
                assertNull(consumer, "Thread 2 should timeout");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();
    limitedPool.shutdownPool();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBorrowConsumerWithNullTopic() throws InterruptedException {
    kafkaConsumerPool.borrowConsumer(1000, TimeUnit.MILLISECONDS, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBorrowConsumerWithEmptyTopic() throws InterruptedException {
    kafkaConsumerPool.borrowConsumer(1000, TimeUnit.MILLISECONDS, "");
  }

  @Test
  public void testInvalidConsumerRemovedFromPool() throws InterruptedException {
    KafkaConsumer<String, GenericRecord> invalidConsumer = mock(KafkaConsumer.class);
    when(invalidConsumer.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));
    when(consumerFactory.createConsumer()).thenReturn(invalidConsumer);

    KafkaConsumerPool pool =
        new KafkaConsumerPool(consumerFactory, 1, 1, Duration.ofSeconds(2), null);

    KafkaConsumer<String, GenericRecord> consumer =
        pool.borrowConsumer(500, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    assertNull(consumer, "Should return null when all consumers are invalid");
    verify(invalidConsumer, atLeastOnce()).close();
  }

  @Test
  public void testNewlyCreatedInvalidConsumerRemoved() throws InterruptedException {
    // Reset the mock factory to clear any setup from @BeforeMethod
    reset(consumerFactory);

    KafkaConsumer<String, GenericRecord> invalidConsumer = mock(KafkaConsumer.class);
    when(invalidConsumer.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));
    when(consumerFactory.createConsumer()).thenReturn(invalidConsumer);

    KafkaConsumerPool pool =
        new KafkaConsumerPool(consumerFactory, 0, 1, Duration.ofSeconds(2), null);

    KafkaConsumer<String, GenericRecord> consumer =
        pool.borrowConsumer(1000, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    assertNull(consumer, "Should return null when newly created consumer is invalid");
    // With maxPoolSize=1, after closing an invalid consumer, totalConsumersCreated becomes 0,
    // so we can create another one. This continues until timeout, so multiple consumers may be
    // created.
    verify(invalidConsumer, atLeastOnce()).close();
  }

  @Test
  public void testConsecutiveInvalidConsumerCircuitBreaker() throws InterruptedException {
    KafkaConsumer<String, GenericRecord> invalidConsumer1 = mock(KafkaConsumer.class);
    KafkaConsumer<String, GenericRecord> invalidConsumer2 = mock(KafkaConsumer.class);
    KafkaConsumer<String, GenericRecord> invalidConsumer3 = mock(KafkaConsumer.class);
    KafkaConsumer<String, GenericRecord> invalidConsumer4 = mock(KafkaConsumer.class);

    when(invalidConsumer1.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));
    when(invalidConsumer2.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));
    when(invalidConsumer3.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));
    when(invalidConsumer4.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));

    when(consumerFactory.createConsumer())
        .thenReturn(invalidConsumer1)
        .thenReturn(invalidConsumer2)
        .thenReturn(invalidConsumer3)
        .thenReturn(invalidConsumer4);

    KafkaConsumerPool pool =
        new KafkaConsumerPool(consumerFactory, 2, 2, Duration.ofSeconds(2), null);

    long startTime = System.currentTimeMillis();
    KafkaConsumer<String, GenericRecord> consumer =
        pool.borrowConsumer(2000, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    long elapsedTime = System.currentTimeMillis() - startTime;

    assertNull(consumer, "Should return null when all consumers are invalid");
    assertTrue(
        elapsedTime >= 1000,
        "Should wait at least 1 second due to circuit breaker after maxConsecutiveInvalid (poolSize + 1 = 3)");
    verify(invalidConsumer1, atLeastOnce()).close();
    verify(invalidConsumer2, atLeastOnce()).close();
    verify(invalidConsumer3, atLeastOnce()).close();
  }

  @Test
  public void testMetricsRecordedForInvalidConsumer() throws InterruptedException {
    MetricUtils metricUtils = mock(MetricUtils.class);
    KafkaConsumer<String, GenericRecord> invalidConsumer = mock(KafkaConsumer.class);
    when(invalidConsumer.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));
    when(consumerFactory.createConsumer()).thenReturn(invalidConsumer);

    KafkaConsumerPool pool =
        new KafkaConsumerPool(consumerFactory, 1, 1, Duration.ofSeconds(2), metricUtils);

    pool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    verify(metricUtils, atLeastOnce())
        .increment(eq(KafkaConsumerPool.class), eq("invalid_consumer_found"), eq(1.0));
  }

  @Test
  public void testValidConsumerAfterInvalidOnes() throws InterruptedException {
    // Reset the mock factory to clear any setup from @BeforeMethod
    reset(consumerFactory);

    KafkaConsumer<String, GenericRecord> invalidConsumer1 = mock(KafkaConsumer.class);
    KafkaConsumer<String, GenericRecord> invalidConsumer2 = mock(KafkaConsumer.class);
    KafkaConsumer<String, GenericRecord> validConsumer = mock(KafkaConsumer.class);
    when(invalidConsumer1.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));
    when(invalidConsumer2.assignment()).thenThrow(new IllegalStateException("Consumer is closed"));
    when(validConsumer.assignment()).thenReturn(Collections.emptySet());
    when(validConsumer.partitionsFor(anyString(), any(Duration.class)))
        .thenReturn(Collections.emptyList());

    when(consumerFactory.createConsumer())
        .thenReturn(invalidConsumer1)
        .thenReturn(invalidConsumer2)
        .thenReturn(validConsumer);

    KafkaConsumerPool pool =
        new KafkaConsumerPool(consumerFactory, 0, 3, Duration.ofSeconds(2), null);

    KafkaConsumer<String, GenericRecord> consumer =
        pool.borrowConsumer(5000, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    // Verify the factory was called the expected number of times
    verify(consumerFactory, times(3)).createConsumer();

    assertNotNull(consumer, "Should eventually get a valid consumer");
    assertEquals(consumer, validConsumer, "Should return the valid consumer");
    verify(invalidConsumer1, times(1)).close();
    verify(invalidConsumer2, times(1)).close();
    verify(validConsumer, never()).close();
  }

  @Test
  public void testKafkaExceptionTreatedAsValid() throws InterruptedException {
    KafkaConsumer<String, GenericRecord> consumerWithKafkaException = mock(KafkaConsumer.class);
    when(consumerWithKafkaException.assignment()).thenReturn(Collections.emptySet());
    when(consumerWithKafkaException.partitionsFor(anyString(), any(Duration.class)))
        .thenThrow(new KafkaException("Transient error"));

    when(consumerFactory.createConsumer()).thenReturn(consumerWithKafkaException);

    KafkaConsumerPool pool =
        new KafkaConsumerPool(consumerFactory, 0, 1, Duration.ofSeconds(2), null);

    KafkaConsumer<String, GenericRecord> consumer =
        pool.borrowConsumer(1000, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    assertNotNull(consumer, "Should treat KafkaException as valid (transient)");
    verify(consumerWithKafkaException, never()).close();
  }

  @AfterClass
  public void testShutdownPool() {
    // Call shutdown
    kafkaConsumerPool.shutdownPool();

    // Verify all consumers are closed
    verify(kafkaConsumer, atLeastOnce()).close();
  }
}

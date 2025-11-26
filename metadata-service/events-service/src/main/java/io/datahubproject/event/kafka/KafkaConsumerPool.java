package io.datahubproject.event.kafka;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.springframework.kafka.core.ConsumerFactory;

@Slf4j
public class KafkaConsumerPool {

  private final BlockingQueue<KafkaConsumer<String, GenericRecord>> consumerPool;
  private final ConsumerFactory<String, GenericRecord> consumerFactory;
  private final int maxPoolSize;
  private final Duration validationTimeout;
  @Nullable private final MetricUtils metricUtils;
  @Getter private final AtomicInteger totalConsumersCreated = new AtomicInteger(0);
  @Getter private final Set<KafkaConsumer<String, GenericRecord>> activeConsumers = new HashSet<>();
  @Getter private volatile boolean shuttingDown = false;

  private final ReentrantLock activeConsumersLock = new ReentrantLock();
  private final ReentrantLock poolManagementLock = new ReentrantLock();

  public KafkaConsumerPool(
      final ConsumerFactory<String, GenericRecord> consumerFactory,
      final int initialPoolSize,
      final int maxPoolSize,
      final Duration validationTimeout,
      @Nullable final MetricUtils metricUtils) {
    this.consumerFactory = consumerFactory;
    this.maxPoolSize = maxPoolSize;
    this.validationTimeout = validationTimeout;
    this.metricUtils = metricUtils;
    this.consumerPool = new LinkedBlockingQueue<>(maxPoolSize);

    // Initialize the pool with initial consumers
    for (int i = 0; i < initialPoolSize; i++) {
      consumerPool.add(createConsumer());
    }
  }

  // Create a new consumer when required
  private KafkaConsumer<String, GenericRecord> createConsumer() {
    totalConsumersCreated.incrementAndGet();
    KafkaConsumer<String, GenericRecord> consumer =
        (KafkaConsumer<String, GenericRecord>) consumerFactory.createConsumer();

    activeConsumersLock.lock();
    try {
      activeConsumers.add(consumer);
    } finally {
      activeConsumersLock.unlock();
    }

    return consumer;
  }

  // Borrow a consumer from the pool
  @Nullable
  public KafkaConsumer<String, GenericRecord> borrowConsumer(
      long time, TimeUnit timeUnit, @Nonnull String topic) throws InterruptedException {
    if (shuttingDown) {
      return null;
    }
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("Topic must be non-null and non-empty");
    }

    KafkaConsumer<String, GenericRecord> consumer = null;
    long remainingTime = timeUnit.toMillis(time);
    long startTime = System.currentTimeMillis();
    int consecutiveInvalidCount = 0;
    final int maxConsecutiveInvalid = maxPoolSize + 1;

    while (consumer == null && remainingTime > 0) {
      KafkaConsumer<String, GenericRecord> candidate = consumerPool.poll();

      if (candidate != null) {
        if (isConsumerValid(candidate, topic)) {
          consumer = candidate;
          consecutiveInvalidCount = 0;
        } else {
          log.warn("Found invalid consumer in pool, closing and removing it");
          recordInvalidConsumer();
          closeAndRemoveConsumer(candidate);
          consecutiveInvalidCount++;
        }
      }

      if (consumer == null) {
        // Calculate remaining time once per iteration
        long elapsedTime = System.currentTimeMillis() - startTime;
        remainingTime = timeUnit.toMillis(time) - elapsedTime;

        boolean canCreateMore = false;
        poolManagementLock.lock();
        try {
          canCreateMore = totalConsumersCreated.get() < maxPoolSize && !shuttingDown;
          if (canCreateMore) {
            if (consecutiveInvalidCount >= maxConsecutiveInvalid) {
              log.error(
                  "Too many consecutive invalid consumers ({}), possible Kafka connectivity issue. "
                      + "Waiting before retrying.",
                  consecutiveInvalidCount);
              if (remainingTime > 0) {
                try {
                  Thread.sleep(Math.min(1000, remainingTime));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return null;
                }
                consecutiveInvalidCount = 0;
              }
            } else {
              consumer = createConsumer();
              if (consumer != null && !isConsumerValid(consumer, topic)) {
                log.warn("Newly created consumer is invalid, closing and removing it");
                recordInvalidConsumer();
                closeAndRemoveConsumer(consumer);
                consumer = null;
                consecutiveInvalidCount++;
              } else if (consumer != null) {
                consecutiveInvalidCount = 0;
              }
            }
          }
        } finally {
          poolManagementLock.unlock();
        }

        if (consumer == null && !canCreateMore) {
          // Only wait on the pool if we can't create more consumers
          if (remainingTime > 0) {
            candidate = consumerPool.poll(remainingTime, TimeUnit.MILLISECONDS);
            if (candidate != null && isConsumerValid(candidate, topic)) {
              consumer = candidate;
              consecutiveInvalidCount = 0;
            } else if (candidate != null) {
              log.warn("Found invalid consumer while waiting, closing and removing it");
              recordInvalidConsumer();
              closeAndRemoveConsumer(candidate);
              consecutiveInvalidCount++;
            }
          }
        }
      }
    }

    return consumer;
  }

  private boolean isConsumerValid(
      KafkaConsumer<String, GenericRecord> consumer, @Nonnull String topic) {
    if (consumer == null) {
      return false;
    }

    try {
      consumer.assignment();
      consumer.partitionsFor(topic, validationTimeout);
      return true;
    } catch (IllegalStateException e) {
      log.debug("Consumer validation failed: consumer is closed or invalid", e);
      return false;
    } catch (KafkaException e) {
      log.debug(
          "Consumer validation encountered Kafka exception (may be transient): {}", e.getMessage());
      return true;
    } catch (Exception e) {
      log.debug("Consumer validation encountered exception (may be transient): {}", e.getMessage());
      return true;
    }
  }

  private void recordInvalidConsumer() {
    if (metricUtils != null) {
      metricUtils.increment(this.getClass(), "invalid_consumer_found", 1);
    }
  }

  public void returnConsumer(KafkaConsumer<String, GenericRecord> consumer) {
    if (consumer == null) {
      return;
    }

    // Verify this is actually one of our consumers
    boolean isOurConsumer;
    activeConsumersLock.lock();
    try {
      isOurConsumer = activeConsumers.contains(consumer);
    } finally {
      activeConsumersLock.unlock();
    }

    if (!isOurConsumer) {
      // Not our consumer, don't add to pool
      return;
    }

    if (shuttingDown) {
      // Pool is shutting down, close the consumer instead of returning it
      closeAndRemoveConsumer(consumer);
      return;
    }

    // Try to return to pool, if it fails close the consumer
    if (!consumerPool.offer(consumer)) {
      closeAndRemoveConsumer(consumer);
    }
  }

  private void closeAndRemoveConsumer(KafkaConsumer<String, GenericRecord> consumer) {
    try {
      consumer.close();
    } finally {
      activeConsumersLock.lock();
      try {
        activeConsumers.remove(consumer);
      } finally {
        activeConsumersLock.unlock();
      }
      // Decrement outside activeConsumersLock to avoid deadlock with poolManagementLock
      // This is safe because we're only decrementing an AtomicInteger
      poolManagementLock.lock();
      try {
        totalConsumersCreated.decrementAndGet();
      } finally {
        poolManagementLock.unlock();
      }
    }
  }

  // Shutdown all consumers
  public void shutdownPool() {
    poolManagementLock.lock();
    try {
      shuttingDown = true;
    } finally {
      poolManagementLock.unlock();
    }

    // Create a copy to avoid concurrent modification during iteration
    Set<KafkaConsumer<String, GenericRecord>> consumersToClose;
    activeConsumersLock.lock();
    try {
      consumersToClose = new HashSet<>(activeConsumers);
      activeConsumers.clear();
    } finally {
      activeConsumersLock.unlock();
    }

    // Close all consumers outside the lock to avoid re-acquiring it
    poolManagementLock.lock();
    try {
      for (KafkaConsumer<String, GenericRecord> consumer : consumersToClose) {
        try {
          consumer.close();
        } catch (Exception e) {
          log.warn("Error closing consumer during shutdown", e);
        }
        totalConsumersCreated.decrementAndGet();
      }
    } finally {
      poolManagementLock.unlock();
    }

    consumerPool.clear();
  }
}

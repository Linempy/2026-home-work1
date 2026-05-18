package company.vk.edu.distrib.compute.linempy.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class LinempyAuditService implements AuditService {

    private static final String TOPIC = "audit";
    private static final Logger log = LoggerFactory.getLogger(LinempyAuditService.class);

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final Path storageFile;
    private final List<AuditEvent> events = new ArrayList<>();
    private final ReentrantLock eventsLock = new ReentrantLock();

    private KafkaConsumer<String, String> consumer;
    private Thread consumerThread;
    private final AtomicBoolean running = new AtomicBoolean();

    public LinempyAuditService(String bootstrapServers, String storagePath, String consumerGroupId)
            throws IOException {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.storageFile = Path.of(storagePath, consumerGroupId, "events.log");
        loadPersistedEvents();
    }

    @Override
    public synchronized void start() {
        if (running.get()) {
            return;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        running.set(true);

        consumerThread = new Thread(this::consumeLoop, "audit-consumer-" + consumerGroupId);
        consumerThread.setDaemon(true);
        consumerThread.start();
        log.info("AuditService started, groupId={}", consumerGroupId);
    }

    @Override
    public synchronized void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        if (consumer != null) {
            consumer.wakeup();
        }
        if (consumerThread != null) {
            try {
                consumerThread.join(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        consumerThread = null;
        log.info("AuditService stopped, groupId={}", consumerGroupId);
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        eventsLock.lock();
        try {
            return List.copyOf(events);
        } finally {
            eventsLock.unlock();
        }
    }

    private void consumeLoop() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.isEmpty()) {
                    continue;
                }

                processRecords(records);
            }
        } catch (WakeupException e) {
            if (running.get()) {
                log.error("Audit consumer wakeup error", e);
            }
        } catch (Exception e) {
            log.error("Audit consumer failed", e);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception e) {
                log.debug("Final offset commit skipped: {}", e.getMessage());
            }
        }
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        boolean hasNewEvents = false;
        for (ConsumerRecord<String, String> record : records) {
            try {
                appendEvent(AuditEventCodecUtils.deserialize(record.value()));
                hasNewEvents = true;
            } catch (IOException e) {
                log.error("Failed to persist audit event", e);
            }
        }
        if (hasNewEvents) {
            consumer.commitSync();
        }
    }

    private void appendEvent(AuditEvent event) throws IOException {
        eventsLock.lock();
        try {
            events.add(event);
            persistEvent(event);
        } finally {
            eventsLock.unlock();
        }
    }

    private void loadPersistedEvents() throws IOException {
        if (!Files.exists(storageFile)) {
            return;
        }
        List<String> lines = Files.readAllLines(storageFile, StandardCharsets.UTF_8);
        eventsLock.lock();
        try {
            for (String line : lines) {
                if (!line.isBlank()) {
                    events.add(AuditEventCodecUtils.deserialize(line));
                }
            }
        } finally {
            eventsLock.unlock();
        }
    }

    private void persistEvent(AuditEvent event) throws IOException {
        Files.createDirectories(storageFile.getParent());
        Files.writeString(
                storageFile,
                AuditEventCodecUtils.serialize(event) + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }
}

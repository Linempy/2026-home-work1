package company.vk.edu.distrib.compute.linempy.audit;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.linempy.KVServiceImpl;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class LinempyAuditableKVService extends KVServiceImpl implements AuditableKVService {
    private static final Logger log = LoggerFactory.getLogger(LinempyAuditableKVService.class);
    private static final String TOPIC = "audit";

    private KafkaProducer<String, String> producer;
    private final AtomicBoolean asyncMode = new AtomicBoolean(true);
    private String bootstrapServers;

    public LinempyAuditableKVService(Dao<byte[]> dao, int port) throws IOException {
        super(dao, port);
    }

    private void initProducer() {
        if (producer != null) {
            producer.close();
        }
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
        if (log.isInfoEnabled()) {
            log.info("AuditableKVService started, bootstrapServers={}", bootstrapServers);
        }

    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        initProducer();
    }

    @Override
    public void setAsync(boolean async) {
        asyncMode.set(async);
        if (log.isInfoEnabled()) {
            log.info("Audit mode set to: {}", async ? "async" : "sync");
        }
    }

    @Override
    protected void entityHandler(HttpExchange exchange) throws IOException {
        long timestamp = System.currentTimeMillis();
        String method = exchange.getRequestMethod();
        String id = detachedId(exchange.getRequestURI().getQuery());

        sendAuditEvent(method, id, timestamp);

        super.entityHandler(exchange);
    }

    private void sendAuditEvent(String method, String id, long timestamp) {
        if (producer == null) {
            log.warn("Audit producer is not configured, event skipped");
            return;
        }

        try {
            AuditEvent event = new AuditEvent(method, id, timestamp);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC, id, AuditEventCodecUtils.serialize(event)
            );

            if (asyncMode.get()) {
                sendAsync(record);
            } else {
                sendSync(record);
            }
        } catch (Exception e) {
            log.error("Failed to send audit event", e);
        }
    }

    private void sendAsync(ProducerRecord<String, String> record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send audit event asynchronously", exception);
            } else if (log.isDebugEnabled()) {
                log.debug("Audit event sent to {}:{}", metadata.topic(), metadata.offset());
            }
        });
    }

    private void sendSync(ProducerRecord<String, String> record) {
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            if (log.isDebugEnabled()) {
                log.debug("Audit event sent to {}:{} (sync)", metadata.topic(), metadata.offset());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting for Kafka response", e);
        } catch (ExecutionException e) {
            log.error("Failed to send audit event synchronously", e);
        }
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
        super.stop();
    }
}

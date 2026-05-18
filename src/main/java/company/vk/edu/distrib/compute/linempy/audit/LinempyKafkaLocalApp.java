package company.vk.edu.distrib.compute.linempy.audit;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.linempy.DaoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class LinempyKafkaLocalApp {

    private static final Logger log = LoggerFactory.getLogger(LinempyKafkaLocalApp.class);

    private static final String KAFKA = "localhost:9096";
    private static final String GROUP = "local-audit-cg";

    private LinempyKafkaLocalApp() {
    }

    public static void main(String[] args) {
        try {
            LinempyAuditableKVService kv = new LinempyAuditableKVService(new DaoImpl<>(), 8080);
            kv.setBootstrapServers(KAFKA);
            kv.setAsync(true);

            LinempyAuditServiceFactoryImpl auditFactory = new LinempyAuditServiceFactoryImpl();
            AuditService audit = auditFactory.create(KAFKA, GROUP);

            audit.start();
            kv.start();

            displayInfo();
            registerShutdownHook(kv, audit);
            Thread.sleep(Long.MAX_VALUE);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Application interrupted", e);
        } catch (IOException e) {
            log.error("Failed to start application", e);
        }
    }

    private static void displayInfo() {
        log.info("KV: http://localhost:8080/v0/entity?id=test");
        log.info("GET audit: http://localhost:8080/audit");
        log.info("Ctrl+C to stop");
    }

    private static void registerShutdownHook(LinempyAuditableKVService kv, AuditService audit) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kv.stop();
            audit.stop();
        }));
    }
}

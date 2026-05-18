package company.vk.edu.distrib.compute.linempy.audit;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

import java.io.IOException;

public class LinempyAuditServiceFactoryImpl extends AuditServiceFactory {
    private static final String STORAGE_PATH = System.getenv().getOrDefault("AUDIT_STORAGE_PATH", "./audit_data");

    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new LinempyAuditService(bootstrapServers, STORAGE_PATH, consumerGroupId);
    }
}

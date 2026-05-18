package company.vk.edu.distrib.compute.linempy.audit;

import company.vk.edu.distrib.compute.AuditEvent;

final class AuditEventCodecUtils {

    private static final char SEP = '\u001e';

    private AuditEventCodecUtils() {
    }

    static String serialize(AuditEvent event) {
        return event.method() + SEP + event.id() + SEP + event.timestamp();
    }

    static AuditEvent deserialize(String value) {
        int first = value.indexOf(SEP);
        int second = value.indexOf(SEP, first + 1);
        if (first < 0 || second < 0) {
            throw new IllegalArgumentException("Invalid audit event payload: " + value);
        }
        String method = value.substring(0, first);
        String id = value.substring(first + 1, second);
        long timestamp = Long.parseLong(value.substring(second + 1));
        return new AuditEvent(method, id, timestamp);
    }
}

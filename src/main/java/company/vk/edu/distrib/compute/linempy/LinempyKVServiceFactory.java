package company.vk.edu.distrib.compute.linempy;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.linempy.audit.LinempyAuditableKVService;

import java.io.IOException;

/**
 * LinempyKVServiceFactory — фабрика для обработчиков {@link KVService}.
 *
 * @author Linempy
 * @since 28.03.2026
 */
public class LinempyKVServiceFactory extends KVServiceFactory {

    // private static final String DATA_DIR = System.getProperty("persistent.data.dir", "./linempy_data");

    @Override
    protected KVService doCreate(int port) throws IOException {
        // Dao<byte[]> dao = new PersistentDao(DATA_DIR);
        Dao<byte[]> dao = new DaoImpl<>();
        return new LinempyAuditableKVService(dao, port);
    }
}

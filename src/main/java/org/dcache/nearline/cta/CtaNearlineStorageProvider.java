package org.dcache.nearline.cta;

import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CtaNearlineStorageProvider implements NearlineStorageProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(CtaNearlineStorageProvider.class);

    public final static String VERSION;

    static {
        /*
         * get 'version' attribute from the jar's manifest ( if available )
         */
        ProtectionDomain pd = CtaNearlineStorageProvider.class.getProtectionDomain();
        CodeSource cs = pd.getCodeSource();
        URL u = cs.getLocation();

        var v = "<Unknown>";
        try (JarInputStream jis = new JarInputStream(u.openStream())) {
            Manifest m = jis.getManifest();
            if (m != null) {
                Attributes as = m.getMainAttributes();
                v = as.getValue("version") + " " + as.getValue("build-timestamp");
            }
        } catch (IOException e) {
            // bad luck
        }
        VERSION = v;
    }

    public CtaNearlineStorageProvider() {
        LOGGER.info("Initializing : {}", getDescription());
    }

    @Override
    public String getName() {
        return "dcache-cta";
    }

    @Override
    public String getDescription() {
        return "dCache Nearline Storage Driver for CTA. Version: " + VERSION;
    }

    @Override
    public CtaNearlineStorage createNearlineStorage(String type, String name) {
        LOGGER.info("Creating new CtaNearlineStorage - type: {}, name: {}", type, name);
        return new CtaNearlineStorage(type, name);
    }
}

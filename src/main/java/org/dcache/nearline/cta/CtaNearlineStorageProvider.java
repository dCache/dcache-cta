package org.dcache.nearline.cta;

import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;

public class CtaNearlineStorageProvider implements NearlineStorageProvider {

    private static String VERSION = "<Unknown>";

    static {
        /*
         * get 'version' attribute from the jar's manifest ( if available )
         */
        ProtectionDomain pd = CtaNearlineStorageProvider.class.getProtectionDomain();
        CodeSource cs = pd.getCodeSource();
        URL u = cs.getLocation();

        try (JarInputStream jis = new JarInputStream(u.openStream())) {
            Manifest m = jis.getManifest();

            if (m != null) {
                Attributes as = m.getMainAttributes();
                VERSION = as.getValue("version") + " " + as.getValue("build-timestamp");
            }

        } catch (IOException e) {
            // bad luck
        }
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
        return new CtaNearlineStorage(type, name);
    }
}

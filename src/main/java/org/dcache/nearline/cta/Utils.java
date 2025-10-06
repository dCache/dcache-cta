package org.dcache.nearline.cta;

import diskCacheV111.util.Adler32;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This class is a collection of utility methods to reduce code duplication.
 */
public class Utils {
    private Utils(){ /* prevent instantiation */ }


    /**
     * Calculates the ADLER32 checksum of a file.
     *
     * @param file the file to calculate the checksum for
     * @return the ADLER32 checksum of the file
     * @throws IOException if an I/O error occurs
     */
    public static Checksum calculateChecksum(File file) throws IOException {

        ByteBuffer bb = ByteBuffer.allocate(8192);
        var adler = new Adler32();

        try (FileChannel fc = FileChannel.open(file.toPath())) {
            while (true) {
                bb.clear();
                int n = fc.read(bb);
                if (n < 0) {
                    break;
                }
                bb.flip();
                adler.update(bb);
            }

            return new Checksum(ChecksumType.ADLER32, adler.digest());
        }
    }
}

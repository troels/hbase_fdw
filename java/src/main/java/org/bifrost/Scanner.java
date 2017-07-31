package org.bifrost;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Scanner {
    boolean scan(ByteBuffer buf) throws IOException;
    byte[] scan() throws IOException;
}

package org.komamitsu.fluency;

import org.junit.Test;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.buffer.PackedForwardBuffer;
import org.komamitsu.fluency.flusher.SyncFlusher;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.TCPSender;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class FluencyTest
{
    @Test
    public void test()
            throws IOException, InterruptedException
    {
        // Fluency fluency = Fluency.defaultFluency("127.0.0.1", 24224);
        Buffer buffer = new PackedForwardBuffer();
        Sender sender = new TCPSender("127.0.0.1", 24224);
        Fluency fluency = new Fluency.Builder(sender).setBuffer(buffer).
                setFlusher(new SyncFlusher(buffer, sender)).build();
        Map<String, Object> hashMap = new HashMap<String, Object>();
        hashMap.put("name", "komamitsu");
        hashMap.put("age", 42);
        hashMap.put("email", "komamitsu@gmail.com");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            fluency.emit("foodb.bartbl", hashMap);
        }
        fluency.close();
        System.out.println(System.currentTimeMillis() - start);
    }
}
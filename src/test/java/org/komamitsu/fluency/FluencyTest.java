package org.komamitsu.fluency;

import org.junit.Test;

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
        Fluency fluency = Fluency.defaultFluency("127.0.0.1", 24224);
        Map<String, Object> hashMap = new HashMap<String, Object>();
        hashMap.put("name", "komamitsu");
        hashMap.put("age", 42);
        hashMap.put("email", "komamitsu@gmail.com");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            fluency.emit("foodb.bartbl", hashMap);
        }
        fluency.close();
        System.out.println(System.currentTimeMillis() - start);
    }
}
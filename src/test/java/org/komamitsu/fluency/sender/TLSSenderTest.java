package org.komamitsu.fluency.sender;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeThat;

public class TLSSenderTest
{
    private ObjectMapper objectMapper;

    static class RealSSLFluentdConfig
    {
        @JsonProperty("port")
        public final Integer port;
        @JsonProperty("tag")
        public final String tag;
        @JsonProperty("keystore_path")
        public final String keystorePath;
        @JsonProperty("store_password")
        public final String storePassword;
        @JsonProperty("key_password")
        public final String keyPassword;

        public RealSSLFluentdConfig(
                @JsonProperty("port") Integer port,
                @JsonProperty("tag") String tag,
                @JsonProperty("keystore_path") String keystorePath,
                @JsonProperty("store_password") String storePassword,
                @JsonProperty("key_password") String keyPassword)
        {
            this.port = port == null ? Integer.valueOf(24224) : port;
            this.tag = tag == null ? "foodb.bartbl" : tag;
            this.keystorePath = keystorePath;
            this.storePassword = storePassword;
            this.keyPassword = keyPassword;
        }
    }

    @Before
    public void setUp()
            throws Exception
    {
        objectMapper = new ObjectMapper();
    }

    RealSSLFluentdConfig getConfig()
            throws IOException
    {
        String conf = System.getenv("WITH_SSL_FLUENTD");
        if (conf == null) {
            return null;
        }

        return objectMapper.readValue(conf, RealSSLFluentdConfig.class);
    }

    @Test
    public void withRealFluentd()
            throws IOException
    {
        RealSSLFluentdConfig config = getConfig();

        assumeNotNull(config);

        Fluency fluency = new Fluency.Builder(
                new TLSSender.Config()
                        .setPort(config.port)
                        .setKeystorePath(config.keystorePath)
                        .setStorePassword(config.storePassword)
                        .setKeyPassword(config.keyPassword)
                        .createInstance()
        ).build();
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("name", "komamitsu");
        event.put("age", 42);
        event.put("comment", "hello, world");
        fluency.emit(config.tag, event);
        fluency.close();
    }
}
package com.dynatrace.opentelemetry.metric;

import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OneAgentMetadataEnricherTest {
    Logger logger = Logger.getLogger("TestLogger");

    @Test
    public void validMetrics() {
        OneAgentMetadataEnricher enricher = new OneAgentMetadataEnricher(logger);

        ArrayList<AbstractMap.SimpleEntry<String, String>> entries =
                new ArrayList<>(enricher.parseOneAgentMetadata(Arrays.asList(
                        "prop.a=value.a",
                        "prop.b=value.b"
                )));

        assertEquals("prop.a", entries.get(0).getKey());
        assertEquals("value.a", entries.get(0).getValue());
        assertEquals("prop.b", entries.get(1).getKey());
        assertEquals("value.b", entries.get(1).getValue());
    }

    @Test
    public void invalidMetrics() {
        OneAgentMetadataEnricher enricher = new OneAgentMetadataEnricher(Logger.getLogger("invalidMetricsNameTestLogger"));

        assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("=0x5c14d9a68d569861")).isEmpty());
        assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("key_no_value=")).isEmpty());
        assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("===============")).isEmpty());
        assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("")).isEmpty());
        assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("=")).isEmpty());
        assertTrue(enricher.parseOneAgentMetadata(Collections.emptyList()).isEmpty());

    }
}


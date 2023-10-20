package org.mifos.connector.ams;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtil {

    public Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void test1() {
        logger.info(UUID.randomUUID().toString());
    }
}

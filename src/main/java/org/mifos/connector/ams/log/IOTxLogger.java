package org.mifos.connector.ams.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service("ioTxLogger")
public class IOTxLogger {

    private final Logger logger = LoggerFactory.getLogger("wire");

    public void receiving(String message) {
        logger.debug(">> {}", message);
    }
    
    public void sending(String message) {
    	logger.debug("<< {}", message);
    }
}

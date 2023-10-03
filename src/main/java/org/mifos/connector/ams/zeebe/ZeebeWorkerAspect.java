package org.mifos.connector.ams.zeebe;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.mifos.connector.ams.log.LogInternalCorrelationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class ZeebeWorkerAspect {
    private static final Logger LOG = LoggerFactory.getLogger("zeebe-worker-aspect");

    @Pointcut("execution(@io.camunda.zeebe.spring.client.annotation.JobWorker * *(..))")
    public void zeebeWorkerMethods() {
    }

    @LogInternalCorrelationId
    @Around("zeebeWorkerMethods()")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        LOG.info("## worker entry: {}", joinPoint.getSignature().getName());
        long start = System.currentTimeMillis();
        Object result = joinPoint.proceed();
        long end = System.currentTimeMillis();
        LOG.info("## worker exit: {}, took {} ms", joinPoint.getSignature().getName(), (end - start));
        return result;
    }
}
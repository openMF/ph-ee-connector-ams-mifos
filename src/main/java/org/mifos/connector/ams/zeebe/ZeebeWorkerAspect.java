package org.mifos.connector.ams.zeebe;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
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

    @Around("zeebeWorkerMethods()")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        LOG.info("## worker entry: {}", joinPoint.getSignature().getName());
        Object result = joinPoint.proceed();
        LOG.info("## worker exit: {}", joinPoint.getSignature().getName());
        return result;
    }
}
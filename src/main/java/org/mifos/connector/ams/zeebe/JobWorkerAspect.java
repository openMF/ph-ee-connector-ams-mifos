package org.mifos.connector.ams.zeebe;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class JobWorkerAspect {
    private static final Logger logger = LoggerFactory.getLogger("zeebe-worker");

    @Pointcut("execution(public * *(..)) && @annotation(io.camunda.zeebe.spring.client.annotation.JobWorker)")
    public void jobWorkerMethods() {}

    @Around("jobWorkerMethods()")
    public Object intercept(ProceedingJoinPoint joinPoint) throws Throwable {
        logger.info("## Entering worker " + joinPoint.getSignature().getName());
        Object proceed = joinPoint.proceed();
        logger.info("## Exiting worker " + joinPoint.getSignature().getName());
        return proceed;
    }
}
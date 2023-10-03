package org.mifos.connector.ams.log;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

@Order(1)
@Aspect
@Component
@Slf4j
public class LogInternalCorrelationIdAspect {

    @Around("@annotation(LogInternalCorrelationId)")
    public Object logInternalCorrelationId(ProceedingJoinPoint joinPoint) throws Throwable {
        String internalCorrelationId = getInternalCorrelationId(joinPoint);
        if (internalCorrelationId == null) {
            return joinPoint.proceed();
        }
        MDC.put("internalCorrelationId", internalCorrelationId);
        try {
            return joinPoint.proceed();
        } finally {
            MDC.remove("internalCorrelationId");
        }
    }

    public static String getInternalCorrelationId(ProceedingJoinPoint joinPoint) {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        Object[] args = joinPoint.getArgs();
        Parameter[] parameters = method.getParameters();
        for (int i = 0; i < parameters.length; i++) {
            if ("internalCorrelationId".equals(parameters[i].getName())) {
                if (args[i] != null && args[i] instanceof String) {
                    return (String) args[i];
                } else {
                    log.warn("{}'s method internalCorrelationId argument '{}' is not valid", methodSignature.getName(), args[i]);
                    return null;
                }
            }
        }
        log.warn("method {} had no internalCorrelationId argument", methodSignature.getName());
        return null;
    }
}
package org.mifos.connector.ams.log;

import io.camunda.zeebe.spring.client.annotation.Variable;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

@Aspect
@Component
@Slf4j
public class TraceZeebeArgumentsAspect {

    @Around("@annotation(TraceZeebeArguments)")
    public Object traceZeebeArguments(ProceedingJoinPoint joinPoint) throws Throwable {
        if (log.isTraceEnabled()) {
            log.trace(getLogMessage(joinPoint));
        }
        return joinPoint.proceed();
    }

    public static String getLogMessage(ProceedingJoinPoint joinPoint) {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        Object[] args = joinPoint.getArgs();
        Parameter[] parameters = method.getParameters();
        Annotation[] annotations = method.getParameterAnnotations()[0];
        StringBuilder sb = new StringBuilder();
        sb.append(methodSignature.getName());
        int argCount = 0;
        for (int i = 0; i < args.length; i++) {
            for (Annotation annotation : annotations) {
                if (annotation instanceof Variable) {
                    if (argCount == 0) {
                        sb.append(" called with arguments: ");
                    } else {
                        sb.append(", ");
                    }
                    sb.append(parameters[i].getName());
                    sb.append("=");
                    sb.append(args[i]);
                    argCount++;
                }
            }
        }
        if (argCount == 0) {
            sb.append(" called with no arguments");
        }
        return sb.toString();
    }
}
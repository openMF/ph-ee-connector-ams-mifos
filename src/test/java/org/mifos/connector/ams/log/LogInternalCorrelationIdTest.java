package org.mifos.connector.ams.log;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LogInternalCorrelationIdTest {

    @Test
    public void getLogMessageTest() throws NoSuchMethodException {
        {
            ProceedingJoinPoint joinPoint = getJoinPointMock("method1", new Object[]{"value1", "value2"});
            String s = LogInternalCorrelationIdAspect.getInternalCorrelationId(joinPoint);
            assertEquals("value1", s);
        }
        {
            ProceedingJoinPoint joinPoint = getJoinPointMock("method2", new Object[]{1, null});
            String s = LogInternalCorrelationIdAspect.getInternalCorrelationId(joinPoint);
            assertEquals(null, s);
        }
        {
            ProceedingJoinPoint joinPoint = getJoinPointMock("method3", new Object[]{"value1"});
            String s = LogInternalCorrelationIdAspect.getInternalCorrelationId(joinPoint);
            assertEquals(null, s);
        }
    }

    public void method1(String internalCorrelationId, String arg2) {
    }

    public void method2(Integer internalCorrelationId, String arg2) {
    }

    public void method3(String arg1) {
    }

    private static ProceedingJoinPoint getJoinPointMock(String methodName, Object[] arguments) throws NoSuchMethodException {
        Method method = null;
        for (Method m : LogInternalCorrelationIdTest.class.getMethods()) {
            if (methodName.equals(m.getName())) {
                method = m;
                break;
            }
        }

        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        MethodSignature signature = mock(MethodSignature.class);

        when(joinPoint.getSignature()).thenReturn(signature);
        when(joinPoint.getArgs()).thenReturn(arguments);
        when(signature.getMethod()).thenReturn(method);
        when(signature.getName()).thenReturn(methodName);

        return joinPoint;
    }
}
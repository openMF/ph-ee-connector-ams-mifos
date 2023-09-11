package org.mifos.connector.ams.log;

import io.camunda.zeebe.spring.client.annotation.Variable;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TraceZeebeArgumentsTest {

    @Test
    public void getLogMessageTest() throws NoSuchMethodException {
        {
            ProceedingJoinPoint joinPoint = getJoinPointMock("method1", new Object[]{"value1", "value2"});
            String s = TraceZeebeArgumentsAspect.getLogMessage(joinPoint);
            assertEquals("method1 called with arguments: arg1=value1, arg2=value2", s);
        }
        {
            ProceedingJoinPoint joinPoint = getJoinPointMock("method2", new Object[]{"value1", null});
            String s = TraceZeebeArgumentsAspect.getLogMessage(joinPoint);
            assertEquals("method2 called with arguments: arg1=value1, arg2=null", s);
        }
        {
            ProceedingJoinPoint joinPoint = getJoinPointMock("method3", new Object[]{"value1"});
            String s = TraceZeebeArgumentsAspect.getLogMessage(joinPoint);
            assertEquals("method3 called with no arguments", s);
        }
    }

    public void method1(@Variable String arg1, @Variable String arg2) {
    }

    public void method2(@Variable String arg1, @Variable String arg2) {
    }

    public void method3(String arg1) {
    }

    private static ProceedingJoinPoint getJoinPointMock(String methodName, Object[] arguments) throws NoSuchMethodException {
        Method method = null;
        for (Method m : TraceZeebeArgumentsTest.class.getMethods()) {
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
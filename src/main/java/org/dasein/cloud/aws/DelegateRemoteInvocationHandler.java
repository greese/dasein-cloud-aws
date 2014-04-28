package org.dasein.cloud.aws;

import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.util.APITrace;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
* User: mgulimonov
* Date: 23.04.2014
*/
public class DelegateRemoteInvocationHandler implements InvocationHandler {
    private Object delegate;
    private CloudProvider provider;

    public <T> DelegateRemoteInvocationHandler(T delegate, CloudProvider provider) {
        this.delegate = delegate;
        this.provider = provider;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        LogRequest annotation = method.getAnnotation(LogRequest.class);
        if (annotation != null) {
            APITrace.begin(provider, annotation.value().isEmpty() ? getCallTitle(method) : annotation.value());
        }
        try {
            return method.invoke(delegate, args);
        } finally {
            if (annotation != null) {
                APITrace.end();
            }
        }
    }

    private String getCallTitle(Method method) {
        return delegate.getClass().getSimpleName() + "." + method.getName();
    }

    public static <T> T getInstance(Class<T> clazz, T delegate, CloudProvider provider1) {
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, new DelegateRemoteInvocationHandler(delegate, provider1));
    }
}

package org.mifos.connector.ams.common.util;

import lombok.Value;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

@Value(staticConstructor = "of")
public class BeanWalker<T> {

    private T data;

    public <R> BeanWalker<R> get(Function<T, R> f) {
        R result = null;
        if (data != null) {
            result = f.apply(data);
        }
        return new BeanWalker<R>(result);
    }

    public BeanWalker<T> set(Consumer<T> consumer) {
        T t = this.get();
        if (t != null) {
            consumer.accept(t);
        }
        return this;
    }

    public T get() {
        return data;
    }

    public T result() {
        return data;
    }

    public static <T> Function<List<T>, T> element(int index) {
        return (list) -> {
            if (list != null && list.size() > index) {
                return (T) list.get(index);
            }
            return null;
        };
    }

}

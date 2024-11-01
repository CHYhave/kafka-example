package com.xiaoyu.provider;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chy
 * @description
 * @date 2024/10/29
 */
public class CustomerGenerator {
    public static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);

    public static Customer getNext() {
        int id = ID_GENERATOR.getAndIncrement();
        return new Customer(id, "customer-" + id);
    }
}

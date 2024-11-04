package com.xiaoyu.timingwheel;

import java.util.concurrent.CompletableFuture;

public interface ExpirationService {
    <T> CompletableFuture<T> failAfter(long var1);
}

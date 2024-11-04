package com.xiaoyu.timingwheel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class TimingWheelExpirationService implements ExpirationService {
    public final Timer kafka$raft$TimingWheelExpirationService$$timer;
    private final ExpiredOperationReaper expirationReaper;

    private ExpiredOperationReaper expirationReaper() {
        return this.expirationReaper;
    }

    public <T> CompletableFuture<T> failAfter(final long timeoutMs) {
        TimerTaskCompletableFuture task = new TimerTaskCompletableFuture(timeoutMs);
        task.future().whenComplete((x$1, x$2) -> {
            task.cancel();
        });
        this.kafka$raft$TimingWheelExpirationService$$timer.add(task);
        return task.future();
    }

    public void shutdown() {
        this.expirationReaper().shutdown();
    }

    public TimingWheelExpirationService(final Timer timer) {
        this.kafka$raft$TimingWheelExpirationService$$timer = timer;
        this.expirationReaper = new ExpiredOperationReaper(this);
        this.expirationReaper().start();
    }

    private class ExpiredOperationReaper extends ShutdownableThread {
        public void doWork() {
//            this.kafka$raft$TimingWheelExpirationService$ExpiredOperationReaper$$$outer().kafka$raft$TimingWheelExpirationService$$timer.advanceClock(TimingWheelExpirationService$.MODULE$.kafka$raft$TimingWheelExpirationService$$WorkTimeoutMs());
        }

        public ExpiredOperationReaper(final TimingWheelExpirationService $outer) {
            if ($outer == null) {
                throw null;
            } else {
                this.$outer = $outer;
                super("raft-expiration-reaper", false);
            }
        }
    }

    private static class TimerTaskCompletableFuture<T> extends TimerTask {
        private final long delayMs;
        private final CompletableFuture<T> future;

        public CompletableFuture<T> future() {
            return this.future;
        }

        public void run() {
            this.future().completeExceptionally(new TimeoutException((new StringBuilder(63)).append("Future failed to be completed before timeout of ").append(this.delayMs).append(" ms was reached").toString()));
        }

        public TimerTaskCompletableFuture(final long delayMs) {
            super(delayMs);
            this.delayMs = delayMs;
            this.future = new CompletableFuture();
        }
    }
}

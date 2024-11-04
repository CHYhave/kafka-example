package com.xiaoyu.timingwheel;

public abstract class TimerTask implements Runnable {
    private volatile TimerTaskEntry timerTaskEntry;
    // timestamp in millisecond
    public final long delayMs;

    public TimerTask(long delayMs) {
        this.delayMs = delayMs;
    }

    public void cancel() {
        synchronized (this) {
            if (timerTaskEntry != null) timerTaskEntry.remove();
            timerTaskEntry = null;
        }
    }

    public boolean isCancelled() {
        return timerTaskEntry == null;
    }

    final void setTimerTaskEntry(TimerTaskEntry entry) {
        synchronized (this) {
            // if this timerTask is already held by an existing timer task entry,
            // we will remove such an entry first.
            if (timerTaskEntry != null && timerTaskEntry != entry) {
                timerTaskEntry.remove();
            }

            timerTaskEntry = entry;
        }
    }

    TimerTaskEntry getTimerTaskEntry() {
        return timerTaskEntry;
    }
}

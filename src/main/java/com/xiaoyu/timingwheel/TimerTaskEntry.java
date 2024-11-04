package com.xiaoyu.timingwheel;

public class TimerTaskEntry {
    public final TimerTask timerTask;
    public final long expirationMs;
    volatile TimerTaskList list;
    TimerTaskEntry next;
    TimerTaskEntry prev;

    @SuppressWarnings("this-escape")
    public TimerTaskEntry(
        TimerTask timerTask,
        long expirationMs
    ) {
        this.timerTask = timerTask;
        this.expirationMs = expirationMs;

        // if this timerTask is already held by an existing timer task entry,
        // setTimerTaskEntry will remove it.
        if (timerTask != null) {
            timerTask.setTimerTaskEntry(this);
        }
    }

    public boolean cancelled() {
        return timerTask.getTimerTaskEntry() != this;
    }

    public void remove() {
        TimerTaskList currentList = list;
        // If remove is called when another thread is moving the entry from a task entry list to another,
        // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
        // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
        while (currentList != null) {
            currentList.remove(this);
            currentList = list;
        }
    }
}

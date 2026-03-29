package com.example.ledger.model;

public class AccountActivity {

    private long windowStartMs;
    private int eventsInWindow;
    private long lastEventTime;

    public long getWindowStartMs() {
        return windowStartMs;
    }

    public void setWindowStartMs(long windowStartMs) {
        this.windowStartMs = windowStartMs;
    }

    public int getEventsInWindow() {
        return eventsInWindow;
    }

    public void setEventsInWindow(int eventsInWindow) {
        this.eventsInWindow = eventsInWindow;
    }

    public long getLastEventTime() {
        return lastEventTime;
    }

    public void setLastEventTime(long lastEventTime) {
        this.lastEventTime = lastEventTime;
    }
}


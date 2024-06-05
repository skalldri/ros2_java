package org.ros2.rcljava.time;

public class Duration {
    long seconds;
    int nanos;

    public static final long NS_PER_S = 1000000000;

    private Duration(long sec, int nano) {
        this.seconds = sec;
        this.nanos = nano;
    }

    public int getNano() {
        return this.nanos;
    }

    public long getSeconds() {
        return this.seconds;
    }

    public long toNanos() {
        return (this.seconds * NS_PER_S) + this.nanos;
    }

    public static Duration ofNanos(long nano) {
        return ofSeconds(0, nano);
    }

    public static Duration ofSeconds(long sec, long nano) {
        long final_nanos = (sec * NS_PER_S) + nano;
        long nn = final_nanos % NS_PER_S;
        long ss = ((final_nanos - nn) / NS_PER_S);
        return new Duration(ss, (int) nn);
    }

    public static Duration ofSeconds(long sec) {
        return new Duration(sec, 0);
    }
}

package com.example;

/**
 * Created by powell on 11/19/15.
 */
public class Message {
    public static final long version = -65535L;
    public static final int SIZE = Long.SIZE/8 + Integer.SIZE/8;
    public final Long sid;
    public final Integer value;
    public Message(long sid, int value) {
        this.sid = sid;
        this.value = value;
    }
}

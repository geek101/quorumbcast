package com.example;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by powell on 12/13/15.
 */
public class ConcurrentList {
    @Test
    public void testToArray() {
        ConcurrentLinkedQueue<Long> q = new ConcurrentLinkedQueue<>();
        q.add(1L);
        q.add(2L);

        Collection<Long> votes = Arrays.asList(q.toArray(new Long[0]));
        Iterator<Long> it = votes.iterator();
        assertEquals("1 exists", 1L, it.next().longValue());
        assertEquals("2 exists", 2L, it.next().longValue());

        assertFalse("empty list", q.size() == 0);
    }
}

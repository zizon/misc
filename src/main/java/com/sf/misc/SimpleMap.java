package com.sf.misc;

import java.util.Arrays;
import java.util.Iterator;

public class SimpleMap implements Iterable<Object[]> {

    private Object[] buckets;

    public SimpleMap() {
        this.buckets = new Object[10];
    }

    public void inc(char key, long count) {
        int hash = (int) key;
        int index = hash % this.buckets.length;

        Object candidate = this.buckets[index];
        if (candidate == null) {
            this.buckets[index] = candidate = new Object[10];
        }

        // do cast
        Object[] keys = (Object[]) candidate;

        // find real key
        boolean found = false;
        for (int i = 0; i < keys.length; i++) {
            Object current = keys[i];
            if (current == null) {
                // no keys yet
                keys[i] = new Object[]{key, count};
                found = true;
                break;
            }
            // find one
            // do cast
            Object[] holder = (Object[]) current;
            if ((char) holder[0] == key) {
                // match
                holder[1] = (long) holder[1] + count;
                found = true;
                break;
            }
        }

        if (found) {
            return;
        }

        // resize
        Object[] new_keys = Arrays.copyOf(keys, keys.length * 2);
        new_keys[keys.length] = new Object[]{key, count};
        this.buckets[index] = new_keys;

        //TODO
        // resize all when keys become too large
    }

    public long get(char key) {
        int hash = (int) key;
        int index = hash % this.buckets.length;

        Object candidate = this.buckets[index];
        if (candidate == null) {
            return 0;
        }

        // do cast
        Object[] keys = (Object[]) candidate;
        for (int i = 0; i < keys.length; i++) {
            Object current = keys[i];
            if (current == null) {
                break;
            }

            // find one
            // do cast
            Object[] holder = (Object[]) current;
            if ((char) holder[0] == key) {
                return (long) holder[1];
            }
        }

        return 0;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new Iterator<Object[]>() {
            int current_bucket_index = 0;
            int tracker = 0;
            Object[] current_bucket = null;

            @Override
            public boolean hasNext() {
                if (current_bucket != null) {
                    // tracker exceeds?
                    if (tracker < current_bucket.length) {
                        if (current_bucket[tracker] != null) {
                            return true;
                        } else {
                            tracker++;
                            return this.hasNext();
                        }
                    }

                    // try next bucket
                    current_bucket_index++;
                    current_bucket = null;
                    tracker = 0;
                    return hasNext();
                }

                // current bucket is null
                // more bucket?
                for (int i = current_bucket_index; i < buckets.length; i++) {
                    Object candidate = buckets[i];
                    if (candidate != null) {
                        current_bucket_index = i;
                        current_bucket = (Object[]) candidate;
                        tracker = 0;
                        break;
                    }
                }

                // found one bucket?
                if (current_bucket == null) {
                    return false;
                }

                // now reload
                return this.hasNext();
            }

            @Override
            public Object[] next() {
                return (Object[]) current_bucket[tracker++];
            }
        };
    }

    public static void main(String[] args) {
        SimpleMap simple = new SimpleMap();
        simple.inc('x', 1);
        simple.inc('x', 1);
        simple.inc('y', 1);
        System.out.println(simple.get('x'));
        System.out.println(simple.get('y'));

        for (Object[] kv : simple) {
            System.out.println(kv[0] + ":" + kv[1]);
        }
    }


}

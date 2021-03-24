package com.SpaceSaving;



import org.apache.storm.shade.com.google.common.base.Objects;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.org.apache.curator.shaded.com.google.common.base.MoreObjects;

import java.util.LinkedList;


class Bucket<T> {

    private LinkedList<Counter<T>> children = Lists.newLinkedList();

    private long value;

    /**
     * Constructor for a bucket.
     * @param value The current value of the bucket
     */
    Bucket(long value) {
        this.value = value;
    }

    public LinkedList<Counter<T>> getChildren() {
        return children;
    }

    public Bucket setChildren(LinkedList<Counter<T>> children) {
        this.children = children;
        return this;
    }

    public long getValue() {
        return value;
    }

    public Bucket setValue(long value) {
        this.value = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bucket<?> bucket = (Bucket<?>) o;
        return value == bucket.value &&
                Objects.equal(children, bucket.children);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(children, value);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("children", children)
                .add("value", value)
                .toString();
    }
}

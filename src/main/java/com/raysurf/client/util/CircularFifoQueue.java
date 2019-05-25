package com.raysurf.client.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

public class CircularFifoQueue<E> extends AbstractCollection<E> implements Queue<E>, Serializable {
    private static final long serialVersionUID = -8423413834657610406L;
    private transient E[] elements;
    private transient int start;
    private transient int end;
    private transient boolean full;
    private final int maxElements;

    public CircularFifoQueue() {
        this(32);
    }

    public CircularFifoQueue(int size) {
        this.start = 0;
        this.end = 0;
        this.full = false;
        if (size <= 0) {
            throw new IllegalArgumentException("The size must be greater than 0");
        } else {
            this.elements = (E[]) new Object[size];
            this.maxElements = this.elements.length;
        }
    }

    public CircularFifoQueue(Collection<? extends E> coll) {
        this(coll.size());
        this.addAll(coll);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(this.size());
        Iterator var2 = this.iterator();

        while(var2.hasNext()) {
            E e = (E) var2.next();
            out.writeObject(e);
        }

    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.elements = (E[]) new Object[this.maxElements];
        int size = in.readInt();

        for(int i = 0; i < size; ++i) {
            this.elements[i] = (E) in.readObject();
        }

        this.start = 0;
        this.full = size == this.maxElements;
        if (this.full) {
            this.end = 0;
        } else {
            this.end = size;
        }

    }

    public int size() {
        int size;
        if (this.end < this.start) {
            size = this.maxElements - this.start + this.end;
        } else if (this.end == this.start) {
            size = this.full ? this.maxElements : 0;
        } else {
            size = this.end - this.start;
        }
        return size;
    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    public boolean isFull() {
        return false;
    }

    public boolean isAtFullCapacity() {
        return this.size() == this.maxElements;
    }

    public int maxSize() {
        return this.maxElements;
    }

    public void clear() {
        this.full = false;
        this.start = 0;
        this.end = 0;
        Arrays.fill(this.elements, (Object)null);
    }

    public boolean add(E element) {
        if (null == element) {
            throw new NullPointerException("Attempted to add null object to queue");
        } else {
            if (this.isAtFullCapacity()) {
                this.remove();
            }

            this.elements[this.end++] = element;
            if (this.end >= this.maxElements) {
                this.end = 0;
            }

            if (this.end == this.start) {
                this.full = true;
            }

            return true;
        }
    }

    public E get(int index) {
        int sz = this.size();
        if (index >= 0 && index < sz) {
            int idx = (this.start + index) % this.maxElements;
            return this.elements[idx];
        } else {
            throw new NoSuchElementException(String.format("The specified index (%1$d) is outside the available range [0, %2$d)", index, sz));
        }
    }

    public boolean offer(E element) {
        return this.add(element);
    }

    public E poll() {
        return this.isEmpty() ? null : this.remove();
    }

    public E element() {
        if (this.isEmpty()) {
            throw new NoSuchElementException("queue is empty");
        } else {
            return this.peek();
        }
    }

    public E peek() {
        return this.isEmpty() ? null : this.elements[this.start];
    }

    public E remove() {
        if (this.isEmpty()) {
            throw new NoSuchElementException("queue is empty");
        } else {
            E element = this.elements[this.start];
            if (null != element) {
                this.elements[this.start++] = null;
                if (this.start >= this.maxElements) {
                    this.start = 0;
                }

                this.full = false;
            }

            return element;
        }
    }

    private int increment(int index) {
        ++index;
        if (index >= this.maxElements) {
            index = 0;
        }
        return index;
    }

    private int decrement(int index) {
        --index;
        if (index < 0) {
            index = this.maxElements - 1;
        }

        return index;
    }

    public Iterator<E> iterator() {
        return new Iterator<E>() {
            private int index;
            private int lastReturnedIndex;
            private boolean isFirst;

            {
                this.index = CircularFifoQueue.this.start;
                this.lastReturnedIndex = -1;
                this.isFirst = CircularFifoQueue.this.full;
            }

            public boolean hasNext() {
                return this.isFirst || this.index != CircularFifoQueue.this.end;
            }

            public E next() {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    this.isFirst = false;
                    this.lastReturnedIndex = this.index;
                    this.index = CircularFifoQueue.this.increment(this.index);
                    return CircularFifoQueue.this.elements[this.lastReturnedIndex];
                }
            }

            public void remove() {
                if (this.lastReturnedIndex == -1) {
                    throw new IllegalStateException();
                } else if (this.lastReturnedIndex == CircularFifoQueue.this.start) {
                    CircularFifoQueue.this.remove();
                    this.lastReturnedIndex = -1;
                } else {
                    int pos = this.lastReturnedIndex + 1;
                    if (CircularFifoQueue.this.start < this.lastReturnedIndex && pos < CircularFifoQueue.this.end) {
                        System.arraycopy(CircularFifoQueue.this.elements, pos, CircularFifoQueue.this.elements, this.lastReturnedIndex, CircularFifoQueue.this.end - pos);
                    } else {
                        while(pos != CircularFifoQueue.this.end) {
                            if (pos >= CircularFifoQueue.this.maxElements) {
                                CircularFifoQueue.this.elements[pos - 1] = CircularFifoQueue.this.elements[0];
                                pos = 0;
                            } else {
                                CircularFifoQueue.this.elements[CircularFifoQueue.this.decrement(pos)] = CircularFifoQueue.this.elements[pos];
                                pos = CircularFifoQueue.this.increment(pos);
                            }
                        }
                    }

                    this.lastReturnedIndex = -1;
                    CircularFifoQueue.this.end = CircularFifoQueue.this.decrement(CircularFifoQueue.this.end);
                    CircularFifoQueue.this.elements[CircularFifoQueue.this.end] = null;
                    CircularFifoQueue.this.full = false;
                    this.index = CircularFifoQueue.this.decrement(this.index);
                }
            }
        };
    }
}


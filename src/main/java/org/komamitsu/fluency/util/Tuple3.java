package org.komamitsu.fluency.util;

public class Tuple3<F, S, T>
{
    private final F first;
    private final S second;
    private final T third;

    public Tuple3(F first, S second, T third)
    {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public F getFirst()
    {
        return first;
    }

    public S getSecond()
    {
        return second;
    }

    public T getThird()
    {
        return third;
    }
}

package de.hpi.dbs1.entities;

import org.jetbrains.annotations.NotNull;

public class ActorCounter implements Comparable
{
    public String name;
    public int count;

    public ActorCounter(String name)
    {
        this.name = name;
        this.count = 1;
    }

    // returns n < 0 if ac has greater count OR equal count but lower name, n > 0 if reversed
    //therefore sorts by count DESC, and within one count, name ASC
    public int compareToActor(@NotNull ActorCounter ac)
    {
        if(this.count != ac.count)
        {
            return this.count - ac.count;
        }
        else
        {
            return this.name.compareTo(ac.name);
        }
    }

    public void increaseCount()
    {
        this.count++;
    }

    @Override
    public int compareTo(@NotNull Object o)
    {
        return 0;
    }
}
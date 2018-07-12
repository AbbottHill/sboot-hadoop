package com.cd.Beans;

import lombok.Data;

import java.io.Serializable;

@Data
public class SecondarySortKey implements Comparable<SecondarySortKey>, Serializable {
    public String firstColumn;
    public String secondColumn;


    @Override
    public int compareTo(SecondarySortKey other) {
        int result = 0;
        int first = Integer.parseInt(this.firstColumn);
        int second = Integer.parseInt(this.secondColumn);
        int otherfirst = Integer.parseInt(other.firstColumn);
        int othersecond = Integer.parseInt(other.secondColumn);
        result = (first - otherfirst == 0)?(second - othersecond): (first - otherfirst);
        return result;
    }
}

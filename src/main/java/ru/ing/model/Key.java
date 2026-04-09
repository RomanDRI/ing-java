package ru.ing.model;

import java.util.Objects;

public class Key {
    private final String value;
    private final int index;

    public Key(String value, int index) {
        this.value = value;
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key key = (Key) o;
        return index == key.index && Objects.equals(value, key.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, index);
    }
}

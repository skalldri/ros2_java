package org.ros2.rcljava.utils;

@FunctionalInterface
public interface Supplier<T> {
   T get();
}
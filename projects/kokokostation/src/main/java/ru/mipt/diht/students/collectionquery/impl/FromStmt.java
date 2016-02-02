package ru.mipt.diht.students.collectionquery.impl;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Created by mikhail on 02.02.16.
 */
public class FromStmt<T> {
    private final Stream<T> data;

    private FromStmt(Stream<T> data) {
        this.data = data;
    }

    public static <T> FromStmt<T> from(Iterable<T> iterable) {
        return new FromStmt<>(Utils.iterableToStream(iterable));
    }

    public static <T> FromStmt<T> from(Stream<T> stream) {
        return new FromStmt<>(stream);
    }

    @SafeVarargs
    public final <R> SelectStmt<T, R> select(Class<R> clazz, Function<T, ?>... s) {
        return new SelectStmt<>(clazz, data, false, null, s);
    }

    @SafeVarargs
    public final <R> SelectStmt<T, R> selectDistinct(Class<R> clazz, Function<T, ?>... s) {
        return new SelectStmt<>(clazz, data, true, null, s);
    }

    /**
     * Selects the only defined expression as is without wrapper.
     *
     * @param s
     * @param <R>
     * @return statement resulting in collection of R
     */
    public final <R> SelectStmt<T, R> select(Function<T, R> s) {
        return new SelectStmt<>(null, data, false, null, s);
    }

    /**
     * Selects the only defined expression as is without wrapper.
     *
     * @param first
     * @param second
     * @param <F>
     * @param <S>
     * @return statement resulting in collection of R
     */
    public final <F, S> SelectStmt<T, Pair<F, S>> select(Function<T, F> first, Function<T, S> second) {
        return new SelectStmt<>(null, data, false, null, first, second);
    }

    /**
     * Selects the only defined expression as is without wrapper.
     *
     * @param s
     * @param <R>
     * @return statement resulting in collection of R
     */
    public final <R> SelectStmt<T, R> selectDistinct(Function<T, R> s) {
        return new SelectStmt<T, R>(null, data, true, null, s);
    }

    public <J> JoinClause<T, J> join(Iterable<J> iterable) {
        return new JoinClause<>(Utils.streamToList(data), Utils.iterableToList(iterable));
    }

    public <J> JoinClause<T, J> join(Stream<J> stream) {
        return new JoinClause<>(Utils.streamToList(data), Utils.streamToList(stream));
    }

    public <J> JoinClause<T, J> join(Query<J> stream) {
        return join(stream.execute());
    }

    public class JoinClause<I, J> {
        List<I> left;
        List<J> right;

        public JoinClause(List<I> left, List<J> right) {
            this.left = left;
            this.right = right;
        }

        public FromStmt<Pair<I, J>> on(BiPredicate<I, J> condition) {
            List<Pair<I, J>> result = new ArrayList<>();

            for (I tItem : left) {
                for (J jItem : right) {
                    if (condition.test(tItem, jItem)) {
                        result.add(new Pair<>(tItem, jItem));
                    }
                }
            }

            return new FromStmt<>(result.stream());
        }

        public <K extends Comparable<?>> FromStmt<Pair<I, J>> on(
                Function<I, K> leftKey,
                Function<J, K> rightKey) {
            List<Pair<I, J>> result = new ArrayList<>();

            Map<K, List<I>> hashMap = new HashMap<>();

            for (I item : left) {
                K key = leftKey.apply(item);
                if (hashMap.containsKey(key)) {
                    hashMap.get(key).add(item);
                } else {
                    hashMap.put(key, Utils.arrayListFromElement(item));
                }
            }

            for (J jItem : right) {
                for (I tItem : hashMap.get(rightKey.apply(jItem))) {
                    result.add(new Pair<>(tItem, jItem));
                }
            }

            return new FromStmt<>(result.stream());
        }
    }
}

package ru.mipt.diht.students.collectionquery.impl;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

public class FromStmtHelper<T, R> {
    private final Stream<T> data;
    private final Context<R> context;

    FromStmtHelper(Stream<T> data, Context<R> context) {
        this.data = data;
        this.context = context;
    }

    @SafeVarargs
    public final SelectStmt<T, R> select(Class<R> clazz, Function<T, ?>... s) {
        return new SelectStmt<>(clazz, data, false, context, s);
    }

    @SafeVarargs
    public final SelectStmt<T, R> selectDistinct(Class<R> clazz, Function<T, ?>... s) {
        return new SelectStmt<>(clazz, data, true, context, s);
    }

    public final SelectStmt<T, R> select(Function<T, R> s) {
        return new SelectStmt<>(null, data, false, context, s);
    }

    public final SelectStmt<T, R> select(Function<T, ?> first, Function<T, ?> second) {
        return new SelectStmt<>(null, data, false, context, first, second);
    }

    public final SelectStmt<T, R> selectDistinct(Function<T, R> s) {
        return new SelectStmt<T, R>(null, data, true, context, s);
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

        public FromStmtHelper<Pair<I, J>, R> on(BiPredicate<I, J> condition) {
            List<Pair<I, J>> result = new ArrayList<>();

            for (I tItem : left) {
                for (J jItem : right) {
                    if (condition.test(tItem, jItem)) {
                        result.add(new Pair<>(tItem, jItem));
                    }
                }
            }

            return new FromStmtHelper<>(result.stream(), context);
        }

        public <K extends Comparable<?>> FromStmtHelper<Pair<I, J>, R> on(
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

            return new FromStmtHelper<>(result.stream(), context);
        }
    }
}
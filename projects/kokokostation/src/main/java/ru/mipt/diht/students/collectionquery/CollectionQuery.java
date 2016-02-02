package ru.mipt.diht.students.collectionquery;

import javafx.util.Pair;
import ru.mipt.diht.students.collectionquery.impl.FromStmtHelper;

import java.lang.reflect.InvocationTargetException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import static ru.mipt.diht.students.collectionquery.Aggregates.avg;
import static ru.mipt.diht.students.collectionquery.Aggregates.count;
import static ru.mipt.diht.students.collectionquery.CollectionQuery.Student.student;
import static ru.mipt.diht.students.collectionquery.Conditions.rlike;
import static ru.mipt.diht.students.collectionquery.OrderByConditions.asc;
import static ru.mipt.diht.students.collectionquery.OrderByConditions.desc;
import static ru.mipt.diht.students.collectionquery.Sources.list;
import static ru.mipt.diht.students.collectionquery.impl.FromStmtHelper.from;

public class CollectionQuery {

    /**
     * Make this code work!
     *
     * @param args
     */
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Iterable<Statistics> statistics =
                FromStmtHelper.<Student, Statistics>from(list(
                        student("ivanov", LocalDate.parse("1986-08-06"), "494"),
                        student("ivanov", LocalDate.parse("1986-08-06"), "494")))
                        .select(Statistics.class, Student::getGroup, count(Student::getGroup), avg(Student::age))
                        .where(rlike(Student::getName, ".*ov").and(s -> s.age() > 20))
                        .groupBy(Student::getGroup)
                        .having(s -> s.getCount() > 0)
                        .orderBy(asc(Student::getGroup), desc(count(Student::getGroup)))
                        .limit(100)
                        .union()
                        .from(list(student("ivanov", LocalDate.parse("1985-08-06"), "494")))
                        .selectDistinct(Statistics.class, s -> "all", count(s -> 1), avg(Student::age))
                        .execute();
        System.out.println(statistics);

        Iterable<Pair<String, String>> mentorsByStudent =
                FromStmtHelper.<Student, Pair<String, String>>from(list(student("ivanov", LocalDate.parse("1985-08-06"), "494")))
                        .join(list(new Group("494", "mr.sidorov")))
                        .on((s, g) -> Objects.equals(s.getGroup(), g.getGroup()))
                        .select(sg -> sg.getKey().getName(), sg -> sg.getValue().getMentor())
                        .execute();
        System.out.println(mentorsByStudent);
    }


    public static class Student {
        private final String name;

        private final LocalDate dateOfBith;

        private final String group;

        public String getName() {
            return name;
        }

        public Student(String name, LocalDate dateOfBith, String group) {
            this.name = name;
            this.dateOfBith = dateOfBith;
            this.group = group;
        }

        public LocalDate getDateOfBith() {
            return dateOfBith;
        }

        public String getGroup() {
            return group;
        }

        public long age() {
            return ChronoUnit.YEARS.between(getDateOfBith(), LocalDateTime.now());
        }

        public static Student student(String name, LocalDate dateOfBith, String group) {
            return new Student(name, dateOfBith, group);
        }
    }

    public static class Group {
        private final String group;
        private final String mentor;

        public Group(String group, String mentor) {
            this.group = group;
            this.mentor = mentor;
        }

        public String getGroup() {
            return group;
        }

        public String getMentor() {
            return mentor;
        }
    }


    public static class Statistics {

        private final String group;
        private final Long count;
        private final Double age;

        public String getGroup() {
            return group;
        }

        public Long getCount() {
            return count;
        }

        public Double getAge() {
            return age;
        }

        public Statistics(String group, Long count, Double age) {
            this.group = group;
            this.count = count;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Statistics{"
                    + "group='" + group + '\''
                    + ", count=" + count
                    + ", age=" + age
                    + '}';
        }
    }

}
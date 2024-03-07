package com.hamza.ch04_createrddusingparallelize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateRddUsingParallelizeTest {

    private final SparkConf sparkConf = new SparkConf()
            .setAppName("CreateRddUsingParallelizeTest")
            .setMaster("local[*]");

    @Test
    @DisplayName("Create an empty rdd with no partitions in Spark")
    void createAnEmptyRddWithNoPartitionsInSpark() {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            final JavaRDD<Object> emptyRdd = sc.emptyRDD();
            System.out.println(emptyRdd);
            System.out.printf("Number of partitions: %d%n", emptyRdd.getNumPartitions());
        }
    }

    @Test
    @DisplayName("Create an empty rdd with default partitions in Spark")
    void createAnEmptyRddWithDefaultPartitionsInSpark() {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            final JavaRDD<Object> emptyRdd = sc.parallelize(List.of());
            System.out.println(emptyRdd);
            System.out.printf("Number of partitions: %d%n", emptyRdd.getNumPartitions());
        }
    }

    @Test
    @DisplayName("Create Spark rdd using parallelize method")
    void createSparkRddUsingParallelizeMethod() {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            final var data = Stream.iterate(1, n -> n + 1).limit(8L).collect(Collectors.toList());
            final JavaRDD<Integer> myRdd = sc.parallelize(data);
            System.out.println(myRdd);
            System.out.printf("Number of partitions: %d%n", myRdd.getNumPartitions());
            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.println("Elements in RDD: ");
            myRdd.collect().forEach(System.out::println);

            // Reduce Operations
            final Integer max = myRdd.reduce(Integer::max);
            final Integer min = myRdd.reduce(Integer::min);
            final Integer sum = myRdd.reduce(Integer::sum);

            System.out.printf("MAX:%d, MIN:%d, SUM:%d", max, min, sum);
        }
    }

    @Test
    @DisplayName("Create Spark rdd using parallelize with given partitions")
    void createSparkRddUsingParallelizeMethodWithGivenPartitions() {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            final var data = Stream.iterate(1, n -> n + 1).limit(8L).collect(Collectors.toList());
            final JavaRDD<Integer> myRdd = sc.parallelize(data, 4);
            System.out.println(myRdd);
            System.out.printf("Number of partitions: %d%n", myRdd.getNumPartitions());
            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.println("Elements in RDD: ");
            myRdd.collect().forEach(System.out::println);

            // Reduce Operations
            final Integer max = myRdd.reduce(Integer::max);
            final Integer min = myRdd.reduce(Integer::min);
            final Integer sum = myRdd.reduce(Integer::sum);

            System.out.printf("MAX:%d, MIN:%d, SUM:%d", max, min, sum);
        }
    }
}
package com.hamza.ch04_createrddusingparallelize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

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
}
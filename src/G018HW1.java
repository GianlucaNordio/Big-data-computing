import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.awt.*;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static com.google.common.primitives.Longs.min;

public class G018HW1{
    static void ExactOutliers(List<Point2D> listOfPoints, double D, int M, int K) {
        System.out.println("Executing ExactOutliers with parameters: D=" + D + ", M=" + M + ", K=" + K);

        // Create a Map to count the number of points under the distance for each point
        Map<Point2D, Long> counts = new HashMap<>();

        for(int i = 0; i < listOfPoints.size(); i++) {
            for (int j = 0; j < listOfPoints.size(); j++) {
                Point2D x = listOfPoints.get(i);
                Point2D y = listOfPoints.get(j);
                if (x.distance(y) <= D) {
                    counts.put(x, 1L + counts.getOrDefault(x, 0L));
                    counts.put(y, 1L + counts.getOrDefault(y, 0L));
                }
            }
        }

        long numberOfOutliers = 0L;

        for( Long l : counts.values()){
            if(l < M)
                numberOfOutliers++;
        }

        System.out.println("The number of sure (D,M)-outliers is " + numberOfOutliers);

        List<Map.Entry<Point2D, Long>> orderedOutliers = new ArrayList<>(counts.entrySet());

        orderedOutliers.sort(Map.Entry.comparingByValue());
        

        for(int i = 0; i < min(K, numberOfOutliers); i++) {
            Point2D point = orderedOutliers.get(i).getKey();
            System.out.println("(" + point.getX() + "," + point.getY() +")");
        }
    }
    public static void MRApproxOutliers(JavaRDD<Point2D> pointsRDD, double D, int M, int K) {
        System.out.println("Executing MRApproxOutliers with parameters: D=" + D + ", M=" + M + ", K=" + K);

        // Step A: Transform input RDD into RDD of non-empty cells
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> cellRDD = pointsRDD.mapToPair(point -> {
            int i = (int) Math.floor(point.getX() / (D / (2 * Math.sqrt(2))));
            int j = (int) Math.floor(point.getY() / (D / (2 * Math.sqrt(2))));
            return new Tuple2<>(new Tuple2<>(i, j), 1);
        }).reduceByKey(Integer::sum);

        List<Tuple2<Tuple2<Integer, Integer>, Integer>> cellList = cellRDD.collect();

        // Step B: Attach |N3(C)| and |N7(C)| to each cell
        JavaRDD<Tuple2<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Integer, Integer>>> cellInfoRDD = cellRDD.map(cell -> {
            int i = cell._1()._1();
            int j = cell._1()._2();
            int N3 = calculateN3(cell._1(), cellList);
            int N7 = calculateN7(cell._1(), cellList);
            return new Tuple2<>(new Tuple2<>(cell._1(), cell._2()), new Tuple2<>(N3, N7));
        });

        // Compute and print results
        long sureOutliers = cellInfoRDD.filter(cell -> {
            int N7 = cell._2()._2();
            return N7 <= M;
        }).count();

        long uncertainPoints = cellInfoRDD.filter(cell -> {
            int N3 = cell._2()._1();
            int N7 = cell._2()._2();
            return (N3 <= M && N7 > M);
        }).count();

        List<Tuple2<Tuple2<Integer, Integer>, Integer>> sortedCells = cellRDD.mapToPair(pair -> new Tuple2<>(pair._2, pair._1 )).sortByKey().mapToPair(pair -> new Tuple2<>(pair._2, pair._1)).take(K);

        // Print results
        System.out.println("Number of sure (" + D + "," + M + ")-outliers: " + sureOutliers);
        System.out.println("Number of uncertain points: " + uncertainPoints);
        System.out.println("First " + K + " non-empty cells:");
        sortedCells.forEach(cell -> System.out.println("Cell " + cell._1() + " Size: " + cell._2()));
    }


    // Helper method to calculate N3
    public static int calculateN3(Tuple2<Integer, Integer> cell, Iterable<Tuple2<Tuple2<Integer, Integer>, Integer>> cellRDD) {
        int i = cell._1();
        int j = cell._2();
        int count = 0;
        for (Tuple2<Tuple2<Integer, Integer>, Integer> neighbor : cellRDD) {
            int x = neighbor._1()._1();
            int y = neighbor._1()._2();
            if ((Math.abs(x - i) <= 1) && (Math.abs(y - j) <= 1) && (x != i || y != j)) {
                count++;
            }
        }
        return count;
    }

    public static int calculateN7(Tuple2<Integer, Integer> cell, Iterable<Tuple2<Tuple2<Integer, Integer>, Integer>> cellRDD) {
        int i = cell._1();
        int j = cell._2();
        int count = 0;
        for (Tuple2<Tuple2<Integer, Integer>, Integer> neighbor : cellRDD) {
            int x = neighbor._1()._1();
            int y = neighbor._1()._2();
            if ((Math.abs(x - i) <= 1) && (Math.abs(y - j) <= 1) && (x != i || y != j)) {
                count++;
            }
        }
        return count;
    }


    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Usage: PointProcessing <inputFilePath> <D> <M> <K> <L>");
            System.exit(1);
        }

        // Retrieve command-line arguments
        String inputFilePath = args[0];
        double D = Double.parseDouble(args[1]);
        int M = Integer.parseInt(args[2]);
        int K = Integer.parseInt(args[3]);
        int L = Integer.parseInt(args[4]);

        // Create a Spark context
        SparkConf conf = new SparkConf(true).setAppName("G018HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // Print command-line arguments
        System.out.println("Input File Path: " + inputFilePath);
        System.out.println("D: " + D);
        System.out.println("M: " + M);
        System.out.println("K: " + K);
        System.out.println("L: " + L);

        // Read input points into an RDD of strings
        JavaRDD<String> rawData = sc.textFile(inputFilePath);

        // Transform into RDD of points (pairs of integers)
        JavaRDD<Point2D> inputPoints = rawData.map(line -> {
            String[] parts = line.split(",");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);
            return new Point2D.Double(x,y);
        });

        // Repartition RDD into L partitions
        inputPoints = inputPoints.repartition(L);

        // Print total number of points
        long totalPoints = inputPoints.count();
        System.out.println("Total number of points: " + totalPoints);

        if (totalPoints <= 200000) {
            // Collect points into a list
            List<Point2D> listOfPoints = inputPoints.collect();

            // Execute ExactOutliers
            long startTimeExact = System.currentTimeMillis();
            ExactOutliers(listOfPoints, D, M, K);
            long endTimeExact = System.currentTimeMillis();
            System.out.println("ExactOutliers running time: " + (endTimeExact - startTimeExact) + " milliseconds");
        }

        // Execute MRApproxOutliers
        long startTimeMRApprox = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M, K);
        long endTimeMRApprox = System.currentTimeMillis();
        System.out.println("MRApproxOutliers running time: " + (endTimeMRApprox - startTimeMRApprox) + " milliseconds");


        // Close Spark context
        sc.close();
    }
}
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.awt.*;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static com.google.common.primitives.Longs.min;

public class G018HW1{
    /*
        Computes the number of exact outliers based on
        - list of points in input
        - distance defining when to count a point as close
        - number of points close in order to not be an outlier
        - number of outliers to show (if enough points are available)
    */
    static void ExactOutliers(List<Point2D> listOfPoints, double D, int M, int K) {
        System.out.println("Executing ExactOutliers with parameters: D=" + D + ", M=" + M + ", K=" + K);

        long startTimeExact = System.currentTimeMillis();

        // Create a map storing (point, number of points which have distance <= D from the point as key)
        Map<Integer, Long> counts = new HashMap<>();

        for(int i = 0; i < listOfPoints.size(); i++) {
            counts.put(i, 1L + counts.getOrDefault(i, 0L));
            for (int j = i + 1; j < listOfPoints.size(); j++) {
                Point2D x = listOfPoints.get(i);
                Point2D y = listOfPoints.get(j);
                if (x.distance(y) <= D) {
                    counts.put(i, 1L + counts.getOrDefault(i, 0L));
                    counts.put(j, 1L + counts.getOrDefault(j, 0L));
                }
            }
        }

        // Compute number of outliers
        long numberOfOutliers = 0L;
        for(Long l : counts.values()){
            if(l <= M)
                numberOfOutliers++;
        }

        long endTimeExact = System.currentTimeMillis();

        System.out.println("The number of sure (D,M)-outliers is " + numberOfOutliers);

        // The first K elements (or the available points) are shown sorted by number of elements at distance <= D
        List<Map.Entry<Integer, Long>> orderedOutliers = new ArrayList<>(counts.entrySet());
        orderedOutliers.sort(Map.Entry.comparingByValue());

        for(int i = 0; i < min(K, numberOfOutliers); i++) {
            int index = orderedOutliers.get(i).getKey();
            Point2D point = listOfPoints.get(index);
            System.out.println("(" + point.getX() + "," + point.getY() +")");
        }

        System.out.println("ExactOutliers running time: " + (endTimeExact - startTimeExact) + " milliseconds");
    }
    public static void MRApproxOutliers(JavaRDD<Point2D> pointsRDD, double D, int M, int K) {
        System.out.println("Executing MRApproxOutliers with parameters: D=" + D + ", M=" + M + ", K=" + K);

        // Input RDD: points
        // Output RDD: (i,j) is the key and number of points in that square is the value
        JavaPairRDD<Tuple2<Long, Long>, Long> cellRDD = pointsRDD.mapToPair(point -> {
            long i = (long) Math.floor(point.getX() / (D / (2 * Math.sqrt(2))));
            long j = (long) Math.floor(point.getY() / (D / (2 * Math.sqrt(2))));
            return new Tuple2<>(new Tuple2<>(i, j), 1L);
        }).reduceByKey(Long::sum).cache();

        // Computation of |N3(C)| and |N7(C)|
        List<Tuple2<Tuple2<Long, Long>, Long>> cellList = cellRDD.collect();

        //TODO check if here we should use a JavaPairRDD instead of a JavaRDD (also change the method map)
        JavaRDD<Tuple2<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Long, Long>>> cellInfoRDD = cellRDD.map(cell -> {
            long N3 = calculateN3(cell._1(), cellList);
            long N7 = calculateN7(cell._1(), cellList);
            return new Tuple2<>(new Tuple2<>(cell._1(), cell._2()), new Tuple2<>(N3, N7));
        });

       // Compute the number of sure outliers
        long sureOutliers = cellInfoRDD.filter(cell -> {
            long N7 = cell._2()._2();
            return N7 <= M;
        }).map(cell -> cell._1()._2()).reduce(Long::sum);


        // Compute the number of uncertain outliers
        long uncertainPoints = cellInfoRDD.filter(cell -> {
            long N3 = cell._2()._1();
            long N7 = cell._2()._2();
            return (N3 <= M && N7 > M);
        }).map(cell -> cell._1()._2()).reduce(Long::sum);

        List<Tuple2<Tuple2<Long, Long>, Long>> sortedCells = cellRDD.mapToPair(pair -> new Tuple2<>(pair._2, pair._1 ))
                .sortByKey()
                .mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
                .take(K);

        // Print results
        System.out.println("Number of sure (" + D + "," + M + ")-outliers: " + sureOutliers);
        System.out.println("Number of uncertain points: " + uncertainPoints);
        System.out.println("First " + K + " non-empty cells:");
        sortedCells.forEach(cell -> System.out.println("Cell " + cell._1() + " Size: " + cell._2()));
    }


    // Computes the number of elements in the area of size 3x3 around a cell
    public static long calculateN3(Tuple2<Long, Long> cell, Iterable<Tuple2<Tuple2<Long, Long>, Long>> cellRDD) {
        long i = cell._1();
        long j = cell._2();
        long count = 0;
        for (Tuple2<Tuple2<Long, Long>, Long> neighbor : cellRDD) {
            long x = neighbor._1()._1();
            long y = neighbor._1()._2();
            long size = neighbor._2();
            if ((Math.abs(x - i) <= 1) && (Math.abs(y - j) <= 1)) {
                count += size;
            }
        }
        return count;
    }

    // Computes the number of elements in the area of size 7x7 around a cell 
    public static long calculateN7(Tuple2<Long, Long> cell, Iterable<Tuple2<Tuple2<Long, Long>, Long>> cellRDD) {
        long i = cell._1();
        long j = cell._2();
        long count = 0;
        for (Tuple2<Tuple2<Long, Long>, Long> neighbor : cellRDD) {
            long x = neighbor._1()._1();
            long y = neighbor._1()._2();
            long size = neighbor._2();
            if ((Math.abs(x - i) <= 3) && (Math.abs(y - j) <= 3)) {
                count += size;
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

            // Execute ExactOutlier
            ExactOutliers(listOfPoints, D, M, K);
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

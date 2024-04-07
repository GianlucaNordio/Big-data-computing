import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.*;
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
    public static class PointCounter implements Comparable<PointCounter>{
        private final double x, y;
        private int numberOfNeighbours;
        public PointCounter(double x, double y) {
            this.x = x;
            this.y = y;
            numberOfNeighbours = 1;
        }

        public void addNeighbour() {
            numberOfNeighbours++;
        }

        @Override
        public int compareTo(@NotNull G018HW1.PointCounter point) {
            return Integer.compare(numberOfNeighbours, point.numberOfNeighbours);
        }

        public double getX() {
            return x;
        }

        public double getY() {
            return y;
        }

        public int getNumberOfNeighbours() {
            return numberOfNeighbours;
        }

        @Override
        public String toString() {
            return "Point: (" + x + "," + y + ")";
        }
    }
    static void ExactOutliers(List<Point2D> listOfPoints, double D, int M, int K) {
        // Create a map storing (point, number of points which have distance <= D from the point as key)
        long[] counts = new long[listOfPoints.size()];

        for(int i = 0; i < listOfPoints.size(); i++) {
            counts[i] += 1;
            for (int j = i + 1; j < listOfPoints.size(); j++) {
                Point2D x = listOfPoints.get(i);
                Point2D y = listOfPoints.get(j);
                if (x.distance(y) <= D) {
                    counts[i] += 1;
                    counts[j] += 1;
                }
            }
        }

        // Compute number of outliers
        long numberOfOutliers = 0L;
        for(long l : counts){
            if(l <= M)
                numberOfOutliers++;
        }

        System.out.println("Number of Outliers = " + numberOfOutliers);

        // The first K elements (or the available points) are shown sorted by number of elements at distance <= D
        List<Tuple2<Integer,Long>> points = new ArrayList<>();
        for(int i = 0; i < listOfPoints.size(); i++) {
            points.add(new Tuple2<>(i, counts[i]));
        }
        points.sort(Comparator.comparing(Tuple2::_2));

        for(int i = 0; i < min(K, numberOfOutliers); i++) {
            int index = points.get(i)._1();
            System.out.println(listOfPoints.get(index));
        }
    }

    static void ExactOutliers2(List<Point2D> listOfPoints, double D, int M, int K) {
        // Create a map storing (point, number of points which have distance <= D from the point as key)
        PointCounter[] points = new PointCounter[listOfPoints.size()];

        for (int i = 0; i < listOfPoints.size(); i++) {
            Point2D p = listOfPoints.get(i);
            points[i] = new PointCounter(p.getX(), p.getY());
        }

        for(int i = 0; i < listOfPoints.size(); i++) {
            for (int j = i + 1; j < listOfPoints.size(); j++) {
                Point2D x = listOfPoints.get(i);
                Point2D y = listOfPoints.get(j);
                if (x.distance(y) <= D) {
                    points[i].addNeighbour();
                    points[j].addNeighbour();
                }
            }
        }

        // Compute number of outliers
        long numberOfOutliers = 0L;
        for(PointCounter p : points){
            if(p.getNumberOfNeighbours() <= M)
                numberOfOutliers++;
        }

        System.out.println("Number of Outliers = " + numberOfOutliers);

        // The first K elements (or the available points) are shown sorted by number of elements at distance <= D
        Arrays.sort(points);

        for(int i = 0; i < min(K, numberOfOutliers); i++) {
            System.out.println(points[i]);
        }
    }

    public static void MRApproxOutliers(JavaRDD<Point2D> pointsRDD, double D, int M, int K) {
        // Input RDD: points
<<<<<<< HEAD
        // Output RDD: cell with coordinates (i,j) is the key and number of points in that cell is the value
=======
        // Output RDD: (i,j) is the key and number of points in that square is the value
//TODO probably wrong, should use mapPartions
>>>>>>> 614790630d6a308049ecb906efad285b2869b535
        JavaPairRDD<Tuple2<Long, Long>, Long> cellRDD = pointsRDD.mapToPair(point -> {
            long i = (long) Math.floor(point.getX() / (D / (2 * Math.sqrt(2))));
            long j = (long) Math.floor(point.getY() / (D / (2 * Math.sqrt(2))));
            return new Tuple2<>(new Tuple2<>(i, j), 1L);
        }).reduceByKey(Long::sum).cache();

        // Computation of |N3(C)| and |N7(C)|
        List<Tuple2<Tuple2<Long, Long>, Long>> cellList = cellRDD.collect();
        Map<Tuple2<Long,Long>, Long> cellMap = new HashMap<>();
        for (Tuple2<Tuple2<Long, Long>, Long> cell : cellList) {
            cellMap.put(cell._1(), cell._2());
        }

        JavaPairRDD<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Long, Long>> cellInfoRDD = cellRDD.mapToPair(cell -> {
            long N3 = calculateN3(cell._1(), cellMap);
            long N7 = calculateN7(cell._1(), cellMap);
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

        List<Tuple2<Long, Tuple2<Long, Long>>> sortedCells = cellRDD.mapToPair(pair -> new Tuple2<>(pair._2, pair._1 ))
                .sortByKey()
                .take(K);

        // Print results
        System.out.println("Number of sure outliers = " + sureOutliers);
        System.out.println("Number of uncertain points = " + uncertainPoints);
        sortedCells.forEach(cell -> System.out.println("Cell: " + cell._2() + "  Size = " + cell._1()));
    }


    // Computes the number of elements in the area of size 3x3 around a cell
    public static long calculateN3(Tuple2<Long, Long> cell, Map<Tuple2<Long,Long>, Long> cellRDD) {
        long i = cell._1();
        long j = cell._2();
        long count = 0;
        for (int x = -1; x <= 1; x++) {
            for (int y = -1; y <= 1; y++) {
                Tuple2<Long, Long> neighbour = new Tuple2<>(i + x, j + y);
                count += cellRDD.getOrDefault(neighbour, 0L);
            }
        }
        return count;
    }

    // Computes the number of elements in the area of size 7x7 around a cell 
    public static long calculateN7(Tuple2<Long, Long> cell, Map<Tuple2<Long,Long>, Long> cellRDD) {
        long i = cell._1();
        long j = cell._2();
        long count = 0;
        for (int x = -3; x <= 3; x++) {
            for (int y = -3; y <= 3; y++) {
                Tuple2<Long, Long> neighbour = new Tuple2<>(i + x, j + y);
                count += cellRDD.getOrDefault(neighbour, 0L);
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
        System.out.println(inputFilePath + " D=" + D + " M=" + M + " K=" + K + " L=" + L);

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
        System.out.println("Number of points = " + totalPoints);

        if (totalPoints <= 200000) {
            // Collect points into a list
            List<Point2D> listOfPoints = inputPoints.collect();

            // Execute ExactOutlier
            long startTimeExact = System.currentTimeMillis();
            ExactOutliers2(listOfPoints, D, M, K);
            long endTimeExact = System.currentTimeMillis();
            System.out.println("Running time of ExactOutliers = " + (endTimeExact - startTimeExact) + " ms");
        }

        // Execute MRApproxOutliers
        long startTimeMRApprox = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M, K);
        long endTimeMRApprox = System.currentTimeMillis();
        System.out.println("Running time of MRApproxOutliers = " + (endTimeMRApprox - startTimeMRApprox) + " ms");

        // Close Spark context
        sc.close();
    }
}

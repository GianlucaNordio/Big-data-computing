import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;
import java.util.*;
import java.util.List;

/**
 * Class G018HW1 implements algorithms for outlier detection using Apache Spark.
 * Group 18
 * Authors: Lorenzo Cazzador, Giovanni Cinel, Gianluca Nordio
 */
public class G018HW1{

    /**
     * If there are fewer points than this threshold, then the exactOutliers method is executed.
     */
    public static final int THRESHOLD_EXACT_OUTLIERS = 200000;

    /**
     * Represents a point along with the count of its neighbors.
     */
    public static class PointCounter implements Comparable<PointCounter>{
        private final float x, y;
        private int numberOfNeighbours;

        /**
         * Initializes a PointCounter object with the given coordinates.
         * @param x The x-coordinate of the point.
         * @param y The y-coordinate of the point.
         */
        public PointCounter(float x, float y) {
            this.x = x;
            this.y = y;
            numberOfNeighbours = 1;
        }

        /**
         * Increments the count of neighbors for this point.
         */
        public void addNeighbour() {
            numberOfNeighbours++;
        }

        @Override
        public int compareTo(@NotNull PointCounter point) {
            return Integer.compare(numberOfNeighbours, point.numberOfNeighbours);
        }

        /**
         * Retrieves the number of neighbors of the point.
         * @return The number of neighbors of the point.
         */
        public int getNumberOfNeighbours() {
            return numberOfNeighbours;
        }

        /**
         * Calculates the square distance between this point and another point.
         * @param p The other point.
         * @return The square distance between this point and the other point.
         */
        public float squareDistance(@NotNull PointCounter p) {
            return (p.x - x) * (p.x - x) + (p.y - y) * (p.y - y);
        }
        @Override
        public String toString() {
            return "Point: (" + x + "," + y + ")";
        }
    }

    /**
     * Computes the number of exact outliers based on specified parameters.
     * @param listOfPoints List of points in input.
     * @param D Distance defining when to count a point as close.
     * @param M Number of points close in order to not be an outlier.
     * @param K Number of outliers to show (if enough points are available).
     */
    static void ExactOutliers(List<Tuple2<Float,Float>> listOfPoints, float D, int M, int K) {
        // Create a PointCounter array to track the count of points within distance D from each point
        PointCounter[] points = new PointCounter[listOfPoints.size()];

        // Assign a point to each element of the array
        for (int i = 0; i < listOfPoints.size(); i++) {
            Tuple2<Float,Float> p = listOfPoints.get(i);
            points[i] = new PointCounter(p._1(), p._2());
        }

        // Fill the array and avoid comparing points multiple times
        for(int i = 0; i < points.length; i++) {
            for (int j = i + 1; j < points.length; j++) {
                PointCounter x = points[i];
                PointCounter y = points[j];
                if (x.squareDistance(y) <= D * D) { // Square distance used to make code more efficient
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

        // The first K elements (or the available points) are shown sorted by number of neighbours
        Arrays.sort(points);
        for(int i = 0; i < Math.min(K, numberOfOutliers); i++) {
            System.out.println(points[i]);
        }
    }

    /**
     * Computes outliers using MapReduce approach based on specified parameters.
     * @param pointsRDD RDD of points.
     * @param D Distance defining when to count a point as close.
     * @param M Number of points close in order to not be an outlier.
     * @param K Number of cells to print.
     */
    public static void MRApproxOutliers(JavaRDD<Tuple2<Float,Float>> pointsRDD, Float D, int M, int K) {

        // Input RDD: points
        // Output RDD: cell with coordinates (i,j) as key and number of points in that cell as value
        JavaPairRDD<Tuple2<Long, Long>, Long> cellRDD = pointsRDD.mapToPair(point -> {
            double cellSide = D / (2 * Math.sqrt(2));
            long i = (long) Math.floor(point._1() / cellSide);
            long j = (long) Math.floor(point._2() / cellSide);
            return new Tuple2<>(new Tuple2<>(i, j), 1L);
        }).reduceByKey(Long::sum).cache();


        List<Tuple2<Tuple2<Long, Long>, Long>> cellList = cellRDD.collect();
        Map<Tuple2<Long,Long>, Long> cellMap = new HashMap<>();
        for (Tuple2<Tuple2<Long, Long>, Long> cell : cellList) {
            cellMap.put(cell._1(), cell._2());
        }

        // Input RDD: cell coordinates as a key with number of points inside as value
        // Output RDD: cell coordinates as a key with pair (number of points in the cell, (N3, N7)) as value
        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Long, Tuple2<Long, Long>>> cellInfoRDD = cellRDD.mapToPair(cell -> {
            long N3 = calculateN3(cell._1(), cellMap);
            long N7 = calculateN7(cell._1(), cellMap);
            return new Tuple2<>(cell._1(), new Tuple2<>(cell._2(), new Tuple2<>(N3, N7)));
        });

       // Compute the number of sure outliers
        long sureOutliers = cellInfoRDD.filter(cell -> {
            long N7 = cell._2()._2()._2();
            return N7 <= M;
        }).map(cell -> cell._2()._1()).reduce(Long::sum);


        // Compute the number of uncertain outliers
        long uncertainPoints = cellInfoRDD.filter(cell -> {
            long N3 = cell._2()._2()._1();
            long N7 = cell._2()._2()._2();
            return (N3 <= M && N7 > M);
        }).map(cell -> cell._2()._1()).reduce(Long::sum);

        List<Tuple2<Long, Tuple2<Long, Long>>> sortedCells = cellRDD.mapToPair(pair -> new Tuple2<>(pair._2, pair._1 ))
                .sortByKey()
                .take(K);

        // Print results
        System.out.println("Number of sure outliers = " + sureOutliers);
        System.out.println("Number of uncertain points = " + uncertainPoints);
        sortedCells.forEach(cell -> System.out.println("Cell: " + cell._2() + "  Size = " + cell._1()));
    }


    /**
     * Computes the number of elements in the area of size 3x3 around a cell.
     * @param cell The cell coordinates.
     * @param cellRDD The map containing cell coordinates as key and number of points inside as value.
     * @return The number of points in the 3x3 area around the cell.
     */
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

    /**
     * Computes the number of elements in the area of size 7x7 around a cell.
     * @param cell The cell coordinates.
     * @param cellRDD The map containing cell coordinates as key and number of points inside as value.
     * @return The number of points in the 7x7 area around the cell.
     */
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

    /**
     * Main method for executing the outlier detection algorithms.
     * @param args Command line arguments: inputFilePath, D, M, K, L.
     */
    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Usage: PointProcessing <inputFilePath> <D> <M> <K> <L>");
            System.exit(1);
        }

        // Retrieve command-line arguments
        String inputFilePath = args[0];
        float D = Float.parseFloat(args[1]);
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
        JavaRDD<Tuple2<Float,Float>> inputPoints = rawData.map(line -> {
            String[] parts = line.split(",");
            float x = Float.parseFloat(parts[0]);
            float y = Float.parseFloat(parts[1]);
            return new Tuple2<>(x, y);
        });

        // Repartition RDD into L partitions
        inputPoints = inputPoints.repartition(L);

        // Print total number of points
        long totalPoints = inputPoints.count();
        System.out.println("Number of points = " + totalPoints);

        if (totalPoints <= THRESHOLD_EXACT_OUTLIERS) {
            // Collect points into a list
            List<Tuple2<Float,Float>> listOfPoints = inputPoints.collect();

            // Execute ExactOutlier
            long startTimeExact = System.currentTimeMillis();
            ExactOutliers(listOfPoints, D, M, K);
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

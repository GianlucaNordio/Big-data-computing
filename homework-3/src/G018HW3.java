
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;

import java.util.concurrent.Semaphore;

public class G018HW3 {

    public static boolean isNotInRange(double value) {
        return value <= 0 || value >= 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: n, phi, epsilon, delta, portExp");
        }

        int n = Integer.parseInt(args[0]);
        float phi = Float.parseFloat(args[1]);
        float epsilon = Float.parseFloat(args[2]);
        float delta = Float.parseFloat(args[3]);
        int portExp = Integer.parseInt(args[4]);

        if( isNotInRange(phi)  || isNotInRange(epsilon) || isNotInRange(delta) ){
            throw new IllegalArgumentException("phi, epsilon and delta must be between 0 and 1");
        }

        System.out.println("Input parameters:");
        System.out.println("n = " + n);
        System.out.println("phi = " + phi);
        System.out.println("epsilon = " + epsilon);
        System.out.println("delta = " + delta);
        System.out.println("portExp = " + portExp);

        SparkConf conf = new SparkConf(true).setMaster("local[*]").setAppName("G018HW3");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(10));
        sc.sparkContext().setLogLevel("ERROR");

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        long[] streamLength = new long[1];
        streamLength[0] = 0L;
        HashMap<Long, Long> histogram = new HashMap<>();
        HashMap<Long, Long> stickySampling = new HashMap<>();
        ArrayList<Long> reservoir = new ArrayList<>();

        int m = (int) Math.ceil(1 / phi);
        Random random = new Random();

        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                .foreachRDD((batch, time) -> {
                    if (streamLength[0] < n) {
                        long batchSize = batch.count();

                        if(batchSize + streamLength[0] > n)
                            batchSize = n - streamLength[0];

                        List<Long> elements = batch.map(Long::parseLong).collect();
                        int counter = 0;
                        for (Long item : elements){
                            counter++;

                            histogram.put(item, histogram.getOrDefault(item, 0L) + 1);

                            if (stickySampling.containsKey(item)) {
                                stickySampling.put(item, stickySampling.get(item) + 1);
                            } else {
                                double x = random.nextDouble();
                                double r = Math.log(1.0 / (phi * delta)) / epsilon;
                                double p = r / n;
                                if (x <= p) {
                                    stickySampling.put(item, 1L);
                                }
                            }

                            int size = reservoir.size();
                            if (size < m) {
                                reservoir.add(item);
                            } else {
                                double x = random.nextDouble();
                                double p = (double) m / (streamLength[0] + counter);
                                if (x <= p) {
                                    reservoir.remove(random.nextInt(size));
                                    reservoir.add(item);
                                }
                            }

                            if(batchSize == counter)
                                break;
                        }

                        streamLength[0] += batchSize;

                        if (streamLength[0] >= n) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        sc.start();
        stoppingSemaphore.acquire();
        sc.stop(false, false);

        System.out.println("Number of items processed = " + streamLength[0]);

        long freqThreshold = (long) Math.floor(phi * streamLength[0]);
        ArrayList<Long> trueFrequentItems = new ArrayList<>();
        
        for (Map.Entry<Long, Long> entry : histogram.entrySet()) {
            if (entry.getValue() >= freqThreshold) {
                trueFrequentItems.add(entry.getKey());
            }
        }
        trueFrequentItems.sort(Long::compareTo);

        System.out.println("Number of true frequent items = " + trueFrequentItems.size());
        System.out.println("True frequent items:");
        for (Long item : trueFrequentItems) {
            System.out.println(item);
        }


        reservoir.sort(Long::compareTo);
        System.out.println("Reservoir sample:");
        for (Long item : reservoir) {
            System.out.println(item);
        }

        System.out.println("Number of items in the hash table used by Sticky Sampling = " + stickySampling.size());
        ArrayList<Long> stickyFrequentItems = new ArrayList<>();
        double stickyThreshold = (phi - epsilon) * n;
        for (Map.Entry<Long, Long> entry : stickySampling.entrySet()) {
            if (entry.getValue() >= stickyThreshold)
                stickyFrequentItems.add(entry.getKey());
        }
        stickyFrequentItems.sort(Long::compareTo);
        System.out.println("Epsilon-Approximate Frequent Items:");
        for (Long item : stickyFrequentItems) {
            System.out.println(item);
        }
    }
}


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.PriorityQueue;

public class G018HW3 {

    public static boolean isNotInRange(double value) {
        return value < 0 || value > 1;
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

        SparkConf conf = new SparkConf(true).setAppName("G018HW3");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(10));
        sc.sparkContext().setLogLevel("ERROR");

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        long[] streamLength = new long[1];
        streamLength[0] = 0L;
        HashMap<Long, Long> histogram = new HashMap<>();
        HashMap<Long, Long> stickySampling = new HashMap<>();
        ArrayList<Long> reservoir = new ArrayList<>();

        int m = (int) Math.floor(1 / phi);
        Random random = new Random();

        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                .foreachRDD((batch, time) -> {
                    if (streamLength[0] < n) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;

                        batch.foreach(s -> {
                            long item = Long.parseLong(s);

                            if (stickySampling.containsKey(item)) {
                                stickySampling.put(item, stickySampling.get(item) + 1);
                            } else {
                                if (random.nextDouble() <= epsilon) {
                                    stickySampling.put(item, 1L);
                                }
                            }

                            int size = reservoir.size();
                            if (size < m) {
                                reservoir.add(item);
                            } else {
                                double p = random.nextDouble();
                                if (p > ((double) m / size)) {
                                    reservoir.remove(random.nextInt(size));
                                    reservoir.add(item);
                                }
                            }
                        });

                        
//                        if (batchSize > 0) {
//                            System.out.println("Batch size at time [" + time + "] is: " + batchSize);
//                        }
                        if (streamLength[0] >= n) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        //System.out.println("Starting streaming engine");
        sc.start();
        //System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        //System.out.println("Stopping the streaming engine");

        sc.stop(false, false);
        //System.out.println("Streaming engine stopped");

        System.out.println("Number of items processed = " + streamLength[0]);

        long freqThreshold = (long) Math.ceil(phi * streamLength[0]);
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

        ArrayList<Long> reservoirSample = new ArrayList<>(reservoir);
        reservoirSample.sort(Long::compareTo);
        System.out.println("Reservoir sample:");
        for (Long item : reservoirSample) {
            System.out.println(item);
        }

        System.out.println("Number of items in the hash table used by Sticky Sampling = " + stickySampling.size());

        ArrayList<Long> stickyFrequentItems = new ArrayList<>();
        for (Map.Entry<Long, Long> entry : stickySampling.entrySet()) {
            if (entry.getValue() >= freqThreshold) {
                stickyFrequentItems.add(entry.getKey());
            }
        }
        stickyFrequentItems.sort(Long::compareTo);
        System.out.println("Epsilon-Approximate Frequent Items:");
        for (Long item : stickyFrequentItems) {
            System.out.println(item);
        }
    }
}
import com.opencsv.CSVWriter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.swagger.client.ApiException;
import io.swagger.client.api.TextbodyApi;
import io.swagger.client.model.TextLine;

public class Client2 {

  public static AtomicInteger totalFailed = new AtomicInteger(0);
  public static AtomicInteger totalSuccess = new AtomicInteger(0);
  public static Integer requests = 0;
  public static BlockingQueue responseQueue;
  //create static blocking queue and push the results to the blocking queue in the consumer response

  public static void main(String[] args) throws InterruptedException, FileNotFoundException {
    TextbodyApi apiInstance = new TextbodyApi();
    apiInstance.getApiClient().setBasePath("http://localhost:8080/test_war/textbody/");

    if(args.length < 1) {
      System.out.println("Error, usage: java ClassName inputfile");
      System.exit(1);
    }
    else {

      Scanner input = new Scanner(System.in);
      System.out.println("Enter the number of threads:");
      int threads = Integer.parseInt(input.next());
      String function = "wordcount";

      BufferedReader file = new BufferedReader(new FileReader(args[0])); //"src/main/resources/bsds-summer-2021-testdata.txt"
      try (Stream<String> stream = Files.lines(Path.of(args[0]), StandardCharsets.UTF_8)) {
        long lineCount = stream.count();
        responseQueue = new ArrayBlockingQueue((int) lineCount);
      } catch (IOException e) {
        e.printStackTrace();
      }

      BlockingQueue queue = new ArrayBlockingQueue(1024);

      Producer producer = new Producer(queue, file);
      Consumer[] consumerArr;
      consumerArr = new Consumer[threads];
      new Thread(producer).start();

      Date date1 = new Date();
      Timestamp start_time = new Timestamp(date1.getTime());
      //add start time

      for (int i = 0; i < threads; ) {
        Consumer consumer = new Consumer(queue, apiInstance, function);
        consumerArr[i] = consumer;
        consumer.start();
        i++;
      }

      for (Consumer t : consumerArr) {
        t.join();
      }

      //end time
      Date date2 = new Date();
      Timestamp end_time = new Timestamp(date2.getTime());

      long mean = 0;
      long[] medianArr = new long[requests];
      long max = 0;
      List<String[]> csvData = new ArrayList<>();
      String[] header = {"Latency", "Start Time", "Request Type", "Response Code"};
      csvData.add(header);


      for (int i = 0; i < requests; i++) {
        String[] vals = (String[]) responseQueue.take();
        mean += Integer.valueOf(vals[0]);
        medianArr[i] = Integer.valueOf(vals[0]);
        csvData.add(vals);
        if (Integer.valueOf(vals[0]) > max) {
          max = Integer.valueOf(vals[0]);
        }
      }

      Arrays.sort(medianArr);
      long median;
      if (medianArr.length % 2 == 0) {
        median = ( medianArr[medianArr.length / 2] + medianArr[medianArr.length / 2 - 1] ) / 2;
      } else {
        median = medianArr[medianArr.length / 2];
      }
      long p99 = medianArr[(int) Math.floor(requests * 0.99)];
      try (CSVWriter writer = new CSVWriter(new FileWriter("src/main/resources/test.csv"))) {
        writer.writeAll(csvData);
      } catch (IOException e) {
        e.printStackTrace();
      }

      mean = mean / requests;
      //calculate wall time
      Long wall_time = Math.abs(start_time.getTime() - end_time.getTime());
      Long throughput = requests / wall_time;
      System.out.println("Wall time: " + wall_time + "\nThroughput: " + throughput + "\nMean: " +
              mean + "\nMedian: " + median + "\nMax: " + max + "\np99: " + p99);
    }
  }

  public static class Producer implements Runnable{

    protected BlockingQueue queue = null;
    protected BufferedReader file = null;

    public Producer(BlockingQueue queue, BufferedReader file) {
      this.queue = queue;
      this.file = file;
    }

    public void run() {
      try {
        String fileLine;
        while((fileLine = file.readLine()) != null) {
          if (fileLine.length() > 0) {
            queue.put(fileLine);
            requests++;
          }
        }
        Consumer.done = true;
      } catch (InterruptedException | IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static class Consumer extends Thread {

    protected BlockingQueue queue = null;
    protected TextbodyApi apiInstance = null;
    protected String function = null;
    public static boolean done = false;

    public Consumer(BlockingQueue queue, TextbodyApi apiInstance, String function) {
      this.queue = queue;
      this.apiInstance = apiInstance;
      this.function = function;
    }

    public void run() {
      while (!queue.isEmpty() || !done) {
        TextLine body = new TextLine();
        try {
          body.setMessage(queue.take().toString());
          Date start = new Date();
          Timestamp start_time = new Timestamp(start.getTime());
          try {
            BigDecimal result = apiInstance.analyzeNewLine(body, function);
            totalSuccess.incrementAndGet();
            Date end = new Date();
            Timestamp end_time = new Timestamp(end.getTime());
            long responseTime = Math.abs(start_time.getTime() - end_time.getTime());
            String[] results = new String[4];
            results[0] = Long.toString(responseTime);
            results[1] = Long.toString(start_time.getTime());
            results[2] = "POST";
            results[3] = "200";
            responseQueue.put(results);

          } catch (ApiException e) {
            totalFailed.incrementAndGet();
            Date end = new Date();
            Timestamp end_time = new Timestamp(end.getTime());
            System.err.println("Exception when calling TextbodyApi#analyzeNewLine");
            long responseTime = Math.abs(start_time.getTime() - end_time.getTime());
            String[] results = new String[4];
            results[0] = Long.toString(responseTime);
            results[1] = Long.toString(start_time.getTime());
            results[2] = "POST";
            results[3] = "400";
            responseQueue.put(results);
            e.printStackTrace();
          }
        } catch (InterruptedException e) {
          totalFailed.incrementAndGet();
          e.printStackTrace();
        }
      }
    }
  }
}

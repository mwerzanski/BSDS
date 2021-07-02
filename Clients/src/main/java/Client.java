import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.TextbodyApi;
import io.swagger.client.model.TextLine;

public class Client {

  public static AtomicInteger totalFailed = new AtomicInteger(0);
  public static AtomicInteger totalSuccess = new AtomicInteger(0);
  public static Integer requests = 0;
  public static AtomicInteger gets = new AtomicInteger(0);
  public static BlockingQueue responseQueue;
  //create static blocking queue and push the results to the blocking queue in the consumer response

  public static void main(String[] args) throws InterruptedException, FileNotFoundException {
    responseQueue = new LinkedBlockingQueue();
    TextbodyApi apiInstance = new TextbodyApi();
    apiInstance.getApiClient().setBasePath("http://localhost:8080/assignment3Server_war_exploded3/");
    // TODO: update base path in Client and Client2
    if (args.length < 1) {
      System.out.println("Error, usage: java ClassName inputfile");
      System.exit(1);
    } else {
      Scanner input = new Scanner(System.in);
      System.out.println("Enter the number of threads:");
      int threads = Integer.parseInt(input.next());
      String function = "wordcount";

      BufferedReader file = new BufferedReader(new FileReader(args[0]));

      BlockingQueue queue = new ArrayBlockingQueue(1024);

      Producer producer = new Producer(queue, file);
      Consumer[] consumerArr;
      consumerArr = new Consumer[threads];
      new Thread(producer).start();

      Date date1 = new Date();
      Timestamp start_time = new Timestamp(date1.getTime());
      //add start time

      getThread getThread = new getThread(apiInstance,"wordcount");
      getThread.start();

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

      long mean = 0;
      long max = 0;

      for (int i = 0; i < gets.get(); i++) {
        String[] vals = (String[]) responseQueue.take();
        mean += Integer.valueOf(vals[0]);
        if (Integer.valueOf(vals[0]) > max) {
          max = Integer.valueOf(vals[0]);
        }
      }

      mean = mean / gets.get();
      //end time
      Date date2 = new Date();
      Timestamp end_time = new Timestamp(date2.getTime());

      System.out.print("\nMean GET response time: " + mean + "\nMax GET response time: " + max );

      System.out.println("start time " + start_time + "    end time " + end_time);
//calculate wall time
      Long wall_time = Math.abs(start_time.getTime() - end_time.getTime());
      Long throughput = requests / ( wall_time / 1000 );
      System.out.println("Wall time: " + wall_time + "\nSuccessful Threads: " + totalSuccess +
              "\nFailed Threads: " + totalFailed + "\nThroughput: " + throughput);
//loop through the consumers and count successful and failed threads
      //loop through the consumers and count successful and failed threads
    }
  }

  public static class getThread extends Thread {
    private String[] listOfGets = {"the","is","when","a","for","in","an","are","were","was"};
    protected TextbodyApi apiInstance = null;
    public static volatile boolean done = false;

    public getThread(TextbodyApi apiInstance, String function) {
      this.apiInstance = apiInstance;
    }

    public void run() {
      while(!done) {
        gets.incrementAndGet();
        for (int i = 0; i < listOfGets.length; i++) {
          Date start = new Date();
          Timestamp start_time = new Timestamp(start.getTime());
          TextLine body = new TextLine();
          try {
            BigDecimal result = apiInstance.getWordCount(listOfGets[i]);
            Date end = new Date();
            Timestamp end_time = new Timestamp(end.getTime());
            long responseTime = Math.abs(start_time.getTime() - end_time.getTime());
            String[] results = new String[2];
            results[0] = Long.toString(responseTime);
            results[1] = Long.toString(start_time.getTime());
            responseQueue.put(results);

          } catch (ApiException | InterruptedException e) {
            Date end = new Date();
            Timestamp end_time = new Timestamp(end.getTime());
            long responseTime = Math.abs(start_time.getTime() - end_time.getTime());
            String[] results = new String[2];
            results[0] = Long.toString(responseTime);
            results[1] = Long.toString(start_time.getTime());
            try {
              responseQueue.put(results);
            } catch (InterruptedException interruptedException) {
              interruptedException.printStackTrace();
            }
            System.err.println("Exception when calling getWordCount");
            e.printStackTrace();
          }
        }
        try {
          sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static class Producer implements Runnable {

    protected BlockingQueue queue = null;
    protected BufferedReader file = null;

    public Producer(BlockingQueue queue, BufferedReader file) {
      this.queue = queue;
      this.file = file;
    }

    public void run() {
      try {
        String fileLine;
        while (( fileLine = file.readLine() ) != null) {
          if (fileLine.length() > 0) {
            queue.put(fileLine);
            requests++;
          }
        }
        Consumer.done = true;
        getThread.done = true;
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
          try {
            BigDecimal result = apiInstance.analyzeNewLine(body, function);// apiInstance.analyzeNewLineWithHttpInfo(body, function);
            totalSuccess.incrementAndGet();
          } catch (ApiException e) {
            totalFailed.incrementAndGet();
            System.err.println("Exception when calling TextbodyApi#analyzeNewLine");
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

package com.tinkerpop.blueprints.impls.ramcloud;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.Reflection;

// Singleton class
public final class PerfMon {
   private static final PerfMon instance = new PerfMon();

   public final long measureAllTimeProp = Long.valueOf(System.getProperty("benchmark.measureAll", "0"));
   private final static Logger log = LoggerFactory.getLogger(PerfMon.class);

   private static final int debug = 0;

   private static long read_latency_sum;
   private static long read_latency_cnt;
   private static int read_flag;

   private static long write_latency_sum;
   private static long write_latency_cnt;
   private static int write_flag;

   private static long serialize_latency_sum;
   private static long serialize_latency_cnt;
   private static int ser_flag;

   private static long deserialize_latency_sum;
   private static long deserialize_latency_cnt;
   private static int deser_flag;

   private static long addsw_time;
   private static long addport_time;
   private static long addlink_time;
   private static long addport_cnt;
   private static long ser_time;
   private static long deser_time;
   private static long write_time;
   private static long read_time;

   public static PerfMon getInstance() {
        return instance;
    }
   private PerfMon(){
   }

   private void clear(){
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
   	read_latency_sum=0L;
   	read_latency_cnt=0L;
   	write_latency_sum=0L;
   	write_latency_cnt=0L;
   	serialize_latency_sum=0L;
   	serialize_latency_cnt=0L;
   	deserialize_latency_sum=0L;
   	deserialize_latency_cnt=0L;
	read_flag=write_flag=deser_flag=ser_flag=0;
        //log.error("flag cleared");
   }
   public void addswitch_start(){
        if(measureAllTimeProp==0)
		return;

        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
	clear();
	addsw_time = System.nanoTime();
   }
   public void addswitch_end(){
        if(measureAllTimeProp==0)
		return;

        long delta;
        long sum;

        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
        delta = System.nanoTime() - addsw_time;
        sum = read_latency_sum + write_latency_sum + serialize_latency_sum +  deserialize_latency_sum;
        log.error("Performance add_switch {} read {} ({}) write {} ({}) serialize {} ({}) deserialize {} ({}) rwsd total {} other {} ({})", 
	delta, read_latency_sum, read_latency_cnt, write_latency_sum, write_latency_cnt, serialize_latency_sum, serialize_latency_cnt, deserialize_latency_sum, deserialize_latency_cnt, sum, delta-sum, ((float)(delta-sum))*100.0/((float) delta));
   }
   public void addport_start(){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
	clear();
        addport_cnt = 0;
	addport_time = System.nanoTime();
   }
   public void addport_incr(){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
        addport_cnt ++;
   }
   public void addport_end(){
        if(measureAllTimeProp==0)
		return;
        long delta;
        long sum;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
        delta = System.nanoTime() - addport_time;
        sum = read_latency_sum + write_latency_sum + serialize_latency_sum +  deserialize_latency_sum;
        log.error("Performance add_port {} ( {} ports ) read {} ({}) write {} ({}) serialize {} ({}) deserialize {} ({}) rwsd total {} other {} ({})", 
	delta, addport_cnt, read_latency_sum, read_latency_cnt, write_latency_sum, write_latency_cnt, serialize_latency_sum, serialize_latency_cnt, deserialize_latency_sum, deserialize_latency_cnt, sum, delta-sum, ((float)(delta-sum))*100.0/((float) delta));
   }
   public void addlink_start(){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
	clear();
	addlink_time = System.nanoTime();
   }
   public void addlink_end(){
        if(measureAllTimeProp==0)
		return;
        long delta;
        long sum;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
        delta = System.nanoTime() - addlink_time;
        sum = read_latency_sum + write_latency_sum + serialize_latency_sum +  deserialize_latency_sum;
        log.error("Performance add_link {} read {} ({}) write {} ({}) serialize {} ({}) deserialize {} ({}) rwsd total {} other {} ({})", 
	delta, read_latency_sum, read_latency_cnt, write_latency_sum, write_latency_cnt, serialize_latency_sum, serialize_latency_cnt, deserialize_latency_sum, deserialize_latency_cnt, sum, delta-sum, ((float)(delta-sum))*100.0/((float) delta));
   }

   public void read_start(String key){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        } 
	read_time=System.nanoTime();

	if ( debug==1 )
            log.error("read start {}", key);
        if ( read_flag != 0){
            log.error("read_start called twice");
	}

	read_flag = 1;
   }
   public void read_end(String key){
        if(measureAllTimeProp==0)
		return;
        long delta;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
        read_latency_sum += System.nanoTime() - read_time;
        read_latency_cnt ++;

	if ( debug==1 )
            log.error("read end {}", key);
        if ( read_flag != 1){
            log.error("read_end called before read_start");
	}
	read_flag = 0;
   }
   public void write_start(String key){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
	write_time = System.nanoTime();

	if ( debug==1 )
            log.error("write start {}", key);
        if ( write_flag != 0){
            log.error("write_start called twice");
	}
	write_flag = 1;
   }
   public void write_end(String key){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
	if ( debug==1 )
            log.error("write end {}", key);
        write_latency_sum += System.nanoTime() - write_time;
        write_latency_cnt ++;
        if ( write_flag != 1){
            log.error("write_end claled before write_start");
	}
	write_flag = 0;
   }
   public void ser_start(String key){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
	ser_time = System.nanoTime();

	if ( debug==1 )
            log.error("ser start {}", key);
        if ( ser_flag != 0 ){
            	log.error("ser_start called twice");
	}
	ser_flag = 1;
   }
   public void ser_end(String key){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
	if ( debug==1 )
            log.error("ser end {}", key);
        serialize_latency_sum += System.nanoTime() - ser_time;
        serialize_latency_cnt ++;
        if ( ser_flag != 1 ){
            	log.error("ser_end called before ser_start");
	}
	ser_flag = 0;

   }
   public void deser_start(String key){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
	deser_time = System.nanoTime();

	if ( debug==1 )
            log.error("deser start {}", key);
	deser_time = System.nanoTime();
        if ( deser_flag != 0){
            log.error("deser_start called twice");
	}
	deser_flag = 1;
   }
   public void deser_end(String key){
        if(measureAllTimeProp==0)
		return;
        if(! Thread.currentThread().getName().equals("main")){
		return;
        }
	if ( debug==1 )
            log.error("deser end {}", key);

        deserialize_latency_sum += System.nanoTime() - deser_time;
        deserialize_latency_cnt ++;
        if ( deser_flag != 1){
            log.error("deser_end called before deser_start");
	}
	deser_flag = 0;
   }
}

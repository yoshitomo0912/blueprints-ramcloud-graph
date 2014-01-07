package com.tinkerpop.blueprints.impls.ramcloud;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Singleton class
public final class PerfMon {
    private static final ThreadLocal<PerfMon> instance = new ThreadLocal<PerfMon>() {
	@Override
	protected PerfMon initialValue() {
	    return new PerfMon();
	}
    };

   public final long measureAllTimeProp = Long.valueOf(System.getProperty("benchmark.measureAll", "0"));
   private final static Logger log = LoggerFactory.getLogger(PerfMon.class);

   private static final int debug = 0;

   private long read_latency_sum;
   private long read_latency_cnt;
   private int read_flag;

   private long write_latency_sum;
   private long write_latency_cnt;
   private int write_flag;

   private long serialize_latency_sum;
   private long serialize_latency_cnt;
   private int ser_flag;

   private long deserialize_latency_sum;
   private long deserialize_latency_cnt;
   private int deser_flag;

   private long addsw_time;
   private long addport_time;
   private long addlink_time;
   private long addport_cnt;
   private long addflowpath_time;
   private long addflowentry_time;
   private long addflowentry_cnt;
   private long ser_time;
   private long deser_time;
   private long write_time;
   private long read_time;

   public static PerfMon getInstance() {
        return instance.get();
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

	clear();
	addsw_time = System.nanoTime();
   }
   public void addswitch_end(){
        if(measureAllTimeProp==0)
		return;

        long delta;
        long sum;

        delta = System.nanoTime() - addsw_time;
        sum = read_latency_sum + write_latency_sum + serialize_latency_sum +  deserialize_latency_sum;
        log.error("Performance add_switch {} read {} ({}) write {} ({}) serialize {} ({}) deserialize {} ({}) rwsd total {} other {} ({})",
	delta, read_latency_sum, read_latency_cnt, write_latency_sum, write_latency_cnt, serialize_latency_sum, serialize_latency_cnt, deserialize_latency_sum, deserialize_latency_cnt, sum, delta-sum, (delta-sum)*100.0/(delta));
   }
   public void addport_start(){
        if(measureAllTimeProp==0)
		return;
	clear();
        addport_cnt = 0;
	addport_time = System.nanoTime();
   }
   public void addport_incr(){
        if(measureAllTimeProp==0)
		return;
        addport_cnt ++;
   }
   public void addport_end(){
        if(measureAllTimeProp==0)
		return;
        long delta;
        long sum;
        delta = System.nanoTime() - addport_time;
        sum = read_latency_sum + write_latency_sum + serialize_latency_sum +  deserialize_latency_sum;
        log.error("Performance add_port {} ( {} ports ) read {} ({}) write {} ({}) serialize {} ({}) deserialize {} ({}) rwsd total {} other {} ({})",
	delta, addport_cnt, read_latency_sum, read_latency_cnt, write_latency_sum, write_latency_cnt, serialize_latency_sum, serialize_latency_cnt, deserialize_latency_sum, deserialize_latency_cnt, sum, delta-sum, (delta-sum)*100.0/(delta));
   }
   public void addlink_start(){
        if(measureAllTimeProp==0)
		return;
	clear();
	addlink_time = System.nanoTime();
   }
   public void addlink_end(){
        if(measureAllTimeProp==0)
		return;
        long delta;
        long sum;
        delta = System.nanoTime() - addlink_time;
        sum = read_latency_sum + write_latency_sum + serialize_latency_sum +  deserialize_latency_sum;
        log.error("Performance add_link {} read {} ({}) write {} ({}) serialize {} ({}) deserialize {} ({}) rwsd total {} other {} ({})",
	delta, read_latency_sum, read_latency_cnt, write_latency_sum, write_latency_cnt, serialize_latency_sum, serialize_latency_cnt, deserialize_latency_sum, deserialize_latency_cnt, sum, delta-sum, (delta-sum)*100.0/(delta));
   }

   public void addflowpath_start(){
	if(measureAllTimeProp==0) return;
	clear();
	addflowpath_time = System.nanoTime();
   }
   public void addflowpath_end(){
       if(measureAllTimeProp==0) return;
       long delta;
       long sum;
       delta = System.nanoTime() - addflowpath_time;
       sum = read_latency_sum + write_latency_sum + serialize_latency_sum +  deserialize_latency_sum;
       log.error("Performance add_flowpath {} read {} ({}) write {} ({}) serialize {} ({}) deserialize {} ({}) rwsd total {} other {} ({})",
	delta, read_latency_sum, read_latency_cnt, write_latency_sum, write_latency_cnt, serialize_latency_sum, serialize_latency_cnt, deserialize_latency_sum, deserialize_latency_cnt, sum, delta-sum, (delta-sum)*100.0/(delta));
   }

   public void addflowentry_start(){
       if(measureAllTimeProp==0) return;
	clear();
	addflowentry_time = System.nanoTime();
   }
   public void addflowentry_incr(){
       if(measureAllTimeProp==0) return;
       addflowentry_cnt++;
   }
   public void addflowentry_end(){
       if(measureAllTimeProp==0) return;
       long delta;
       long sum;
       delta = System.nanoTime() - addflowentry_time;
       sum = read_latency_sum + write_latency_sum + serialize_latency_sum +  deserialize_latency_sum;
       log.error("Performance add_flowentry {} ( {} ports ) read {} ({}) write {} ({}) serialize {} ({}) deserialize {} ({}) rwsd total {} other {} ({})",
	delta, addflowentry_cnt, read_latency_sum, read_latency_cnt, write_latency_sum, write_latency_cnt, serialize_latency_sum, serialize_latency_cnt, deserialize_latency_sum, deserialize_latency_cnt, sum, delta-sum, (delta-sum)*100.0/(delta));
   }

   public void read_start(String key){
        if(measureAllTimeProp==0)
		return;
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

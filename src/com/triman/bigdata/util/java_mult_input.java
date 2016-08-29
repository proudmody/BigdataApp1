package com.triman.bigdata.util; /**
 * Created by proud on 2016/4/1.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class java_mult_input {
    static Configuration conf = null;
    public static HTablePool pool = null;
    public static String tableName = "test1";
    static int count = 200000;
    static{

        //conf = HBaseConfiguration.create();
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.master", "192.168.3.2:60000");
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.3.2");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        conf = HBaseConfiguration.create(HBASE_CONFIG);
    }
    /*
     * Insert Test single thread
     * */
    public static void SingleThreadInsert()throws IOException
    {
        System.out.println("---------开始SingleThreadInsert测试----------");
        long start = System.currentTimeMillis();
        //HTableInterface table = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        table.setAutoFlush(false);
        table.setWriteBufferSize(24*1024*1024);
        //构造测试数据
        List<Put> list = new ArrayList<Put>();
        //int count = 10000;
        byte[] buffer = new byte[350];
        Random rand = new Random();
        for(int i=0;i<count;i++)
        {
            Put put = new Put(String.format("row %d",i).getBytes());
            rand.nextBytes(buffer);
            put.add("f1".getBytes(), null, buffer);
            //wal=false
            list.add(put);
            if(i%10000 == 0)
            {
                table.put(list);
                list.clear();
                table.flushCommits();
            }
        }
        long stop = System.currentTimeMillis();
        //System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer+",count="+count);

        System.out.println("插入数据："+count+"共耗时："+ (stop - start)*1.0/1000+"s");

        System.out.println("---------结束SingleThreadInsert测试----------");
    }
    /*
     * 多线程环境下线程插入函数
     *
     * */
    public static void InsertProcess()throws IOException
    {
        long start = System.currentTimeMillis();
        //HTableInterface table = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        table.setAutoFlush(false);
        table.setWriteBufferSize(24*1024*1024);
        //构造测试数据
        List<Put> list = new ArrayList<Put>();
        //int count = 100000;
        byte[] buffer = new byte[256];
        Random rand = new Random();
        for(int i=0;i<count;i++)
        {
            Put put = new Put(String.format("row %d",i).getBytes());
            rand.nextBytes(buffer);
            put.add("f1".getBytes(), null, buffer);
            //wal=false
            put.setWriteToWAL(false);
            list.add(put);
            if(i%10000 == 0)
            {
                table.put(list);
                list.clear();
                table.flushCommits();
            }
        }
        long stop = System.currentTimeMillis();
        //System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer+",count="+count);

        System.out.println("线程:"+Thread.currentThread().getId()+"插入数据："+count+"共耗时："+ (stop - start)*1.0/1000+"s");
    }


    /*
     * Mutil thread insert test
     * */
    public static void MultThreadInsert() throws InterruptedException
    {
        System.out.println("---------开始MultThreadInsert测试----------");
        long start = System.currentTimeMillis();
        int threadNumber = 20;
        Thread[] threads=new Thread[threadNumber];
        for(int i=0;i<threads.length;i++)
        {
            threads[i]= new ImportThread();
            threads[i].start();
        }
        for(int j=0;j< threads.length;j++)
        {
            (threads[j]).join();
        }
        long stop = System.currentTimeMillis();

        System.out.println("MultThreadInsert："+threadNumber*count+"共耗时："+ (stop - start)*1.0/1000+"s");
        System.out.println("---------结束MultThreadInsert测试----------");
    }

    /**
     * @param args
     */
    public static void main(String[] args)  throws Exception{
        // TODO Auto-generated method stub
        SingleThreadInsert();
        MultThreadInsert();


    }

    public static class ImportThread extends Thread{
        public void HandleThread()
        {
            //this.TableName = "T_TEST_1";


        }
        //
        public void run(){
            try{
                InsertProcess();
            }
            catch(IOException e){
                e.printStackTrace();
            }finally{
                System.gc();
            }
        }
    }

}
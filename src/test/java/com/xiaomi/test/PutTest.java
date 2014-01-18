/**
 * @author 54chen(陈臻) [chenzhen@xiaomi.com czhttp@gmail.com]
 * @since 2011-3-7 下午01:57:43
 */

package com.xiaomi.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public class PutTest {

    private static final Logger logger = Logger.getLogger(PutTest.class.getName());

    public static void main(String[] args) {
        PutTest nPutTest = new PutTest();
        nPutTest.start(args);
    }

    public void start(String[] args) {
        int threadCount = 0;
        try {
            threadCount = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            threadCount = 1;
        }

        ThreadGroup group = new ThreadGroup("NPUT_Benchmark");
        List<Thread> threads = new ArrayList<Thread>();
        for (int x = 0; x < threadCount; ++x) {
            Thread t = new EchoThread(x, group);
            threads.add(t);
            t.start();
        }

    }

    public static class EchoThread extends Thread {

        public EchoThread(int in, ThreadGroup group) {
            super(group, "EchoThread-" + in);
        }

        @Override
        public void run() {
            String field = " `key`, original_url, encrypted_url, visit_count, password_protected, `password`, remark, userDomain ";
            boolean f = true;
            while (f) {
                // f = false;
                Connection conn = null;
                java.sql.Statement statment = null;

                try {
                    try {
                        Class.forName("com.mysql.jdbc.Driver");
                    } catch (ClassNotFoundException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }

                    conn = DriverManager
                            .getConnection(
                                    "jdbc:mysql://192.168.100.52:8066/short?useUnicode=true&characterEncoding=utf-8&useServerPrepStmts=true&useCompression=true",
                                    "root", "3487e498770b9740086144fc03140876");

                    statment = conn.createStatement();
                    statment.executeUpdate("INSERT INTO short_url(" + field
                            + ") VALUES (\"adfefdsff\",\"\",\"\",1,0,\"\",\"\",\"\")");

                    if (statment != null) {
                        try {
                            statment.close();
                        } catch (Exception e) {
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                        }
                    }
                }
                Stat.getInstance().inc();
            }
        }

    }

    public static class Stat {
        private AtomicLong _count = new AtomicLong(0);

        private static Stat _instance = new Stat();

        public static Stat getInstance() {
            return _instance;
        }

        private Stat() {
            _printer = new RatePrinter(_count);
            _printer.setDaemon(true);
            _printer.start();
        }

        public void inc() {
            _count.incrementAndGet();
        }

        private RatePrinter _printer;

        private static class RatePrinter extends Thread {
            private long _last;

            private AtomicLong _c;

            public RatePrinter(AtomicLong c) {
                _c = c;
            }

            @Override
            public void run() {
                while (true) {
                    try {
                        long current = _c.get();
                        System.out.println("Rate: " + (current - _last) + " req/s");
                        _last = current;
                        Thread.sleep(1000L);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}

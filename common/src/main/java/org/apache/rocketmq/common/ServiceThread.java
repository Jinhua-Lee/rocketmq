/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 服务线程工具类
 * 将某个过程以线程的方式执行，就new一个前端对象
 */
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 待运行线程
     */
    private Thread thread;
    /**
     * 线程安全的倒计时工具
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    /**
     * 标识收到了通知
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    protected volatile boolean stopped = false;
    /**
     * 运行的线程是否是守护线程
     */
    protected boolean isDaemon = false;

    /**
     * Make it able to restart the thread
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public abstract String getServiceName();

    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 如果未启动，则设置启动状态
        // 如果启动了，则不操作
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // 开始启动的处理：标识位，线程的创建及start

        stopped = false;

        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 支持打断的关闭方法
     * 让thread线程在当前线程后xx时间执行
     *
     * @param interrupt 是否可中断
     */
    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 如果是开启状态，则关闭，
        // 如果未开启，则不操作
        if (!started.compareAndSet(true, false)) {
            return;
        }
        // 执行关闭：标识位 + 关闭操作

        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        /*
          未收到通知，则等待点 -1
         */
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            // 打断thread线程的执行状态
            if (interrupt) {
                this.thread.interrupt();
            }


            long beginTime = System.currentTimeMillis();
            // 运行线程不是守护线程，则在指定时间后插入执行
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                    + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    @Deprecated
    public void stop(final boolean interrupt) {
        // 未启动，则不管
        if (!started.get()) {
            return;
        }

        // 执行停止操作： 标识位 + 计数停止
        this.stopped = true;

        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        // 若中断，则让thread停止
        if (interrupt) {
            this.thread.interrupt();
        }
    }

    /**
     * 让thread线程停止
     */
    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    /**
     * 计数方式去唤醒当前线程
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 排队运行
     *
     * @param interval
     */
    protected void waitForRunning(long interval) {
        // 如果通知到了，则让当前线程等待结束
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //entry to wait
        // 重置计数器
        waitPoint.reset();

        try {
            // 执行线程的等待
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            // 等待后，将通知过的标识置为false，继续接受通知
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}

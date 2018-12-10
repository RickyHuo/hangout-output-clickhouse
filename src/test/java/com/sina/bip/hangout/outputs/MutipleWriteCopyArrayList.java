package com.sina.bip.hangout.outputs;

import java.util.concurrent.CopyOnWriteArrayList;

public class MutipleWriteCopyArrayList {

    public static void main(String args[]) {
        CopyOnWriteArrayList<Integer> eventList = new CopyOnWriteArrayList<>();

        int i = 1;

        while (i < 10) {
            eventList.add(i);
            i ++;
        }

        Thread thread = new Thread(){
            public void run(){
                consumerList(eventList);
            }
        };

        thread.start();

        while (i < 20) {
            eventList.add(i);
            i ++;
        }

        consumerList(eventList);
    }

    public static synchronized void consumerList(CopyOnWriteArrayList<Integer> eventList) {
        for (Integer j: eventList) {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                System.out.println("Sleep");
            }
            System.out.println("Main" + j.toString());
            eventList.clear();
        }
    }
}

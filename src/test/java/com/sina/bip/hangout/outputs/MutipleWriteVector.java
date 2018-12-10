package com.sina.bip.hangout.outputs;

import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;

public class MutipleWriteVector {

    public static void main(String args[]) {
        Vector<Integer> eventList = new Vector<>();

        int i = 1;

        while (i < 10) {
            eventList.add(i);
            i ++;
        }

        Thread thread = new Thread(){
            public void run(){
                for (Integer j: eventList) {
                    try {
                        Thread.sleep(1);
                    } catch (Exception e) {
                        System.out.println("Sleep");
                    }
                    System.out.println("Thread" + j.toString());
                }
                eventList.clear();
            }
        };

        thread.start();

        while (i < 20) {
            eventList.add(i);
            i ++;
        }

        for (Integer j: eventList) {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                System.out.println("Sleep");
            }
            System.out.println("Main" + j.toString());
        }
    }
}

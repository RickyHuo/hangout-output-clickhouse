package com.sina.bip.hangout.outputs;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MutipleArrayList {

    public static Boolean lock = false;

    public static void main(String args[]) {
        ArrayList<Integer> eventList = new ArrayList<>();

        int i = 1;

        while (i < 10) {
            while (!lock) {
                eventList.add(i);
                break;
            }
            System.out.println("Add item " + i);
            i ++;
        }

        Thread thread = new Thread(){
            public void run(){
                consumerList(eventList, "Thread");
            }
        };
        thread.start();

        while (i < 20) {
            while (!lock) {
                eventList.add(i);
                break;
            }

            System.out.println("Add item " + i);
            i ++;
        }

        consumerList(eventList, "Main");
    }

    public static synchronized void consumerList(ArrayList<Integer> eventList, String tag) {
        lock = true;
        System.out.println("works: " + tag);
        for (Integer j: eventList) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println("Sleep");
            }
            System.out.println(tag + j.toString());
            eventList.clear();
        }
        lock = false;
    }
}

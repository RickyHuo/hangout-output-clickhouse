package com.sina.bip.hangout.outputs;

import sun.awt.windows.ThemeReader;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class MutipleRead {

    public static void main(String args[]) {
        CopyOnWriteArrayList<Integer> eventList = new CopyOnWriteArrayList<>();

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
            }
        };

        thread.start();

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

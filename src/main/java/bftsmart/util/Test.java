/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.util;

import io.netty.util.internal.StringUtil;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

/**
 *
 * @author juninho
 */
public class Test {
    /** The value is used for character storage. */

    private final char value[] = new char[0];

    /** Cache the hash code for the string */

    private int hash; // Default to 0

    public static void main(String args[]) {
        String s1 = "13";
        String s2 = "1" + "3" + 'S';

        System.out.println("s1 = " + s2);
        // int[] ca = {0, 2, 3, 4, 5, 6, 7, 9, 10, 12, 13, 14};
        int[] ca = { 0, 1, 2 };
        int[] cb = { 0, 2, 3, 4, 5, 6, 7, 9, 10, 12, 13, 15 };
        cb[0] = 12;
        String s = "012";
        ca.toString();
        List<Integer> l = new LinkedList<Integer>();
        l.add(0);
        l.add(1);
        l.add(2);

        System.out.println(l.toString());
        System.out.println("1 = " + (char) 49);
        System.out.println("hash for int ca = " + hash(ca));
        // System.out.println("hash for cb = "+hash(cb));
        System.out.println("hash for string = " + s.hashCode());

    }

    public static int hash(int[] arr) {
        int hash = 0;
        int j = 1;
        int exp;
        for (int i = 0; i < arr.length; i++) {
            hash += 31 * hash + (arr[i]) + 48;
            // exp = arr.length-j;
            // if(!willAdditionOverflow(hash, (int) ((arr[i]+48)*Math.pow(31, exp)))){
            // hash+=(arr[i]+48)*Math.pow(31, exp);
            // }else
            // hash = Integer.MIN_VALUE+((int)Math.pow(31, exp)%(int)Math.pow(2,32));
            //
            //
            // exp=exp-(j);
            // j++;
            // if(exp<0)
            // exp=arr.length;
        }
        return hash - 48;
    }

    public static boolean willAdditionOverflow(int left, int right) {
        if (right < 0 && right != Integer.MIN_VALUE) {
            return willSubtractionOverflow(left, -right);
        } else {
            return (~(left ^ right) & (left ^ (left + right))) < 0;
        }
    }

    public static boolean willSubtractionOverflow(int left, int right) {
        if (right < 0) {
            return willAdditionOverflow(left, -right);
        } else {
            return ((left ^ right) & (left ^ (left - right))) < 0;
        }
    }

    public int hashCode() {

        int h = hash;

        if (h == 0 && value.length > 0) {

            char val[] = value;

            for (int i = 0; i < value.length; i++) {

                h = 31 * h + val[i];

            }

            hash = h;

        }

        return h;

    }
}

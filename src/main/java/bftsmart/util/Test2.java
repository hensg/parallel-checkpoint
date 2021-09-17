/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.util;

/**
 *
 * @author juninho
 */
public class Test2 {
    public static void main (String args[]){
        String s1 = "13";
        String s2 = "1"+"3";
        
        System.out.println("s1 = "+s1.length());
        int[] ca = new int[2];
        ca[0] =1;
        ca[1]=6;
        int[]c = new int[2];
        c[0]=1;
        c[1]=3;
        int[] d = new int[2];
        d[0]=1;
        d[1]=3;
        System.out.println("a = "+ca.hashCode());
        System.out.println("b = "+c.hashCode());
        System.out.println("d = "+d.hashCode());
        
    }    
}

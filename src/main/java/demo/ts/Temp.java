/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demo.ts;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author eduardo
 */
public class Temp {

    private static final Logger logger = LoggerFactory.getLogger(Temp.class);

    public static void main(String[] args) {

        /*
         * File f = new File(args[0]); if (!f.exists()) { System.exit(0); }
         */
        load(args[0]);

    }

    private static void load(String path) {
        try {

            FileReader fr = new FileReader(path);

            BufferedReader rd = new BufferedReader(fr);
            String line = null;
            int j = 0;
            LinkedList<Double> l = new LinkedList<Double>();
            int nextSec = 0;
            while (((line = rd.readLine()) != null)) {
                StringTokenizer st = new StringTokenizer(line, " ");
                try {
                    int i = Integer.parseInt(st.nextToken());
                    if (i <= 120) {

                        String t = st.nextToken();

                        double d = Double.parseDouble(t);

                        if (i > nextSec) {

                            logger.info("entrou para i = " + i + " e next sec = " + nextSec);
                            for (int z = nextSec; z < i; z++) {
                                l.add(d);

                            }
                            nextSec = i;

                            logger.info("saiu com i = " + i + " e next sec = " + nextSec);
                        } else {
                            logger.info("nao entrou i = " + i + " e next sec = " + nextSec);
                        }

                        if (i == nextSec) {
                            l.add(d);
                            nextSec++;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed parsing tokens", e.getCause());
                    System.exit(-1);
                }
            }
            fr.close();
            rd.close();

            logger.info("Size: {}", l.size());

            double sum = 0;
            int i;
            for (i = 0; i < l.size(); i++) {
                sum = sum + l.get(i);
            }

            /*
             * double md1 = sum/250; sum = 0; for(i = 251; i < l.size(); i++){ sum = sum +
             * l.get(i); } double md2 = sum/(l.size()-250);
             * 
             * 
             * logger.info("Media: "+((md1+md2)/2));
             */
            logger.info("Sum: {}", sum);
            logger.info("Media: {}", (sum / l.size()));
        } catch (Exception e) {
            logger.error("Temp load failed", e.getCause());
            System.exit(-1);
        }
    }

}

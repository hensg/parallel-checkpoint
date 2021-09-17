/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demo.bftmap;

import parallelism.ClassToThreads;

/**
 *
 * @author eduardo
 */
public class MultipartitionMapping {

        // escritas conflitantes com somente uma partição
        public static final int W1 = 51;
        public static final int W2 = 52;
        public static final int W3 = 53;
        public static final int W4 = 54;
        public static final int W5 = 55;
        public static final int W6 = 56;
        public static final int W7 = 57;
        public static final int W8 = 58;

        // escrita conflitante entre partição 1 e partição 2,3,4...8
        public static final int W12 = 59;
        public static final int W13 = 60;
        public static final int W14 = 61;
        public static final int W15 = 62;
        public static final int W16 = 63;
        public static final int W17 = 64;
        public static final int W18 = 65;

        // escrita conflitante entre partição 2 e partição 3,4...8
        public static final int W23 = 66;
        public static final int W24 = 67;
        public static final int W25 = 68;
        public static final int W26 = 69;
        public static final int W27 = 70;
        public static final int W28 = 71;

        // escrita conflitante entre partição 3 e partição 4,5,6...8
        public static final int W34 = 72;
        public static final int W35 = 73;
        public static final int W36 = 74;
        public static final int W37 = 75;
        public static final int W38 = 76;
        public static final int W45 = 77;
        public static final int W46 = 78;
        public static final int W47 = 79;
        public static final int W48 = 80;
        public static final int W56 = 81;
        public static final int W57 = 82;
        public static final int W58 = 83;
        public static final int W67 = 84;
        public static final int W68 = 85;

        public static final int W78 = 86;
        // escrita confitante com todas as partições
        public static final int GW = 87;

        // leituras conflitantes com somente uma partição
        public static final int R1 = 19;
        public static final int R2 = 20;
        public static final int R3 = 21;
        public static final int R4 = 22;
        public static final int R12 = 23;
        public static final int R13 = 24;
        public static final int R14 = 25;
        public static final int R23 = 26;
        public static final int R24 = 27;
        public static final int R34 = 28;
        public static final int R5 = 29;
        public static final int R6 = 30;
        public static final int R7 = 31;
        public static final int R8 = 32;
        public static final int R56 = 33;
        public static final int R78 = 34;
        public static final int R15 = 35;
        public static final int R16 = 36;
        public static final int R17 = 37;
        public static final int R18 = 38;
        public static final int R25 = 39;
        public static final int R26 = 40;
        public static final int R27 = 41;
        public static final int R28 = 42;
        public static final int R35 = 43;
        public static final int R36 = 44;
        public static final int R37 = 45;
        public static final int R38 = 46;
        public static final int R45 = 47;
        public static final int R46 = 48;
        public static final int R47 = 49;
        public static final int R48 = 50;
        public static final int R57 = 1;
        public static final int R58 = 2;
        public static final int R67 = 3;
        public static final int R123 = 88;
        public static final int R1234 = 89;
        public static final int R12345 = 90;
        public static final int R123456 = 91;
        public static final int R1234567 = 92;
        public static final int R12345678 = 93;
        // leitura conflitante com todas as partições
        public static final int GR = 33;

        public static ClassToThreads[] getM2P8T8() {
                ClassToThreads[] cts = new ClassToThreads[52];
                int[] ids = new int[8];
                // GR
                ids[0] = 0;
                ids[1] = 1;
                ids[2] = 2;
                ids[3] = 3;
                ids[4] = 4;
                ids[5] = 5;
                ids[6] = 6;
                ids[7] = 7;
                cts[0] = new ClassToThreads(GR, ClassToThreads.SYNC, ids);
                cts[1] = new ClassToThreads(GW, ClassToThreads.SYNC, ids);
                // R1 / W1
                ids = new int[1];
                ids[0] = 0;
                cts[2] = new ClassToThreads(R1, ClassToThreads.CONC, ids);
                cts[3] = new ClassToThreads(W1, ClassToThreads.SYNC, ids);

                // R1 / W1
                ids = new int[1];
                ids[0] = 1;
                cts[4] = new ClassToThreads(R2, ClassToThreads.CONC, ids);
                cts[5] = new ClassToThreads(W2, ClassToThreads.SYNC, ids);

                // R1 / W1
                ids = new int[1];
                ids[0] = 2;
                cts[6] = new ClassToThreads(R3, ClassToThreads.CONC, ids);
                cts[7] = new ClassToThreads(W3, ClassToThreads.SYNC, ids);

                // R1 / W1
                ids = new int[1];
                ids[0] = 3;
                cts[8] = new ClassToThreads(R4, ClassToThreads.CONC, ids);
                cts[9] = new ClassToThreads(W4, ClassToThreads.SYNC, ids);

                // R1 / W1
                ids = new int[1];
                ids[0] = 4;
                cts[10] = new ClassToThreads(R5, ClassToThreads.CONC, ids);
                cts[11] = new ClassToThreads(W5, ClassToThreads.SYNC, ids);
                // R1 / W1
                ids = new int[1];
                ids[0] = 5;
                cts[12] = new ClassToThreads(R6, ClassToThreads.CONC, ids);
                cts[13] = new ClassToThreads(W6, ClassToThreads.SYNC, ids);
                // R1 / W1
                ids = new int[1];
                ids[0] = 6;
                cts[14] = new ClassToThreads(R7, ClassToThreads.CONC, ids);
                cts[15] = new ClassToThreads(W7, ClassToThreads.SYNC, ids);
                // R1 / W1
                ids = new int[1];
                ids[0] = 7;
                cts[16] = new ClassToThreads(R8, ClassToThreads.CONC, ids);
                cts[17] = new ClassToThreads(W8, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 1;
                cts[18] = new ClassToThreads(W12, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 2;
                cts[19] = new ClassToThreads(W13, ClassToThreads.SYNC, ids);

                // W14
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 3;
                cts[20] = new ClassToThreads(W14, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 4;
                cts[21] = new ClassToThreads(W15, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 5;
                cts[22] = new ClassToThreads(W16, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 6;
                cts[23] = new ClassToThreads(W17, ClassToThreads.SYNC, ids);
                // W18
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 7;
                cts[24] = new ClassToThreads(W18, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 2;
                cts[25] = new ClassToThreads(W23, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 3;
                cts[26] = new ClassToThreads(W24, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 4;
                cts[27] = new ClassToThreads(W25, ClassToThreads.SYNC, ids);
                // W26
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 5;
                cts[28] = new ClassToThreads(W26, ClassToThreads.SYNC, ids);
                // W26
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 6;
                cts[29] = new ClassToThreads(W27, ClassToThreads.SYNC, ids);
                // W28
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 7;
                cts[30] = new ClassToThreads(W28, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 2;
                ids[1] = 3;
                cts[31] = new ClassToThreads(W34, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 2;
                ids[1] = 4;
                cts[32] = new ClassToThreads(W35, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 2;
                ids[1] = 5;
                cts[33] = new ClassToThreads(W36, ClassToThreads.SYNC, ids);
                // W26
                ids = new int[2];
                ids[0] = 2;
                ids[1] = 6;
                cts[34] = new ClassToThreads(W37, ClassToThreads.SYNC, ids);
                // W26
                ids = new int[2];
                ids[0] = 2;
                ids[1] = 7;
                cts[35] = new ClassToThreads(W38, ClassToThreads.SYNC, ids);
                // W45
                ids = new int[2];
                ids[0] = 3;
                ids[1] = 4;
                cts[36] = new ClassToThreads(W45, ClassToThreads.SYNC, ids);
                // W26
                ids = new int[2];
                ids[0] = 3;
                ids[1] = 5;
                cts[37] = new ClassToThreads(W46, ClassToThreads.SYNC, ids);
                // W26
                ids = new int[2];
                ids[0] = 3;
                ids[1] = 6;
                cts[38] = new ClassToThreads(W47, ClassToThreads.SYNC, ids);
                // W45
                ids = new int[2];
                ids[0] = 3;
                ids[1] = 7;
                cts[39] = new ClassToThreads(W48, ClassToThreads.SYNC, ids);

                // W26
                ids = new int[2];
                ids[0] = 4;
                ids[1] = 5;
                cts[40] = new ClassToThreads(W56, ClassToThreads.SYNC, ids);
                // W26
                ids = new int[2];
                ids[0] = 4;
                ids[1] = 6;
                cts[41] = new ClassToThreads(W57, ClassToThreads.SYNC, ids);
                // W45
                ids = new int[2];
                ids[0] = 4;
                ids[1] = 7;
                cts[42] = new ClassToThreads(W58, ClassToThreads.SYNC, ids);

                // W26
                ids = new int[2];
                ids[0] = 5;
                ids[1] = 6;
                cts[43] = new ClassToThreads(W67, ClassToThreads.SYNC, ids);
                // W26
                ids = new int[2];
                ids[0] = 5;
                ids[1] = 7;
                cts[44] = new ClassToThreads(W68, ClassToThreads.SYNC, ids);
                // W45
                ids = new int[2];
                ids[0] = 6;
                ids[1] = 7;
                cts[45] = new ClassToThreads(W78, ClassToThreads.SYNC, ids);
                // W45
                ids = new int[3];
                ids[0] = 0;
                ids[1] = 1;
                ids[2] = 2;
                cts[46] = new ClassToThreads(R123, ClassToThreads.SYNC, ids);
                // W45
                ids = new int[4];
                ids[0] = 0;
                ids[1] = 1;
                ids[2] = 2;
                ids[3] = 3;
                cts[47] = new ClassToThreads(R1234, ClassToThreads.SYNC, ids);
                // W45
                ids = new int[5];
                ids[0] = 0;
                ids[1] = 1;
                ids[2] = 2;
                ids[3] = 3;
                ids[4] = 4;
                cts[48] = new ClassToThreads(R12345, ClassToThreads.SYNC, ids);
                // W45
                ids = new int[6];
                ids[0] = 0;
                ids[1] = 1;
                ids[2] = 2;
                ids[3] = 3;
                ids[4] = 4;
                ids[5] = 5;
                cts[49] = new ClassToThreads(R123456, ClassToThreads.SYNC, ids);
                // W45
                ids = new int[7];
                ids[0] = 0;
                ids[1] = 1;
                ids[2] = 2;
                ids[3] = 3;
                ids[4] = 4;
                ids[5] = 5;
                ids[6] = 6;
                cts[50] = new ClassToThreads(R1234567, ClassToThreads.SYNC, ids);
                // last
                ids = new int[8];
                ids[0] = 0;
                ids[1] = 1;
                ids[2] = 2;
                ids[3] = 3;
                ids[4] = 4;
                ids[5] = 5;
                ids[6] = 6;
                ids[7] = 7;
                cts[50] = new ClassToThreads(R12345678, ClassToThreads.SYNC, ids);
                // W26
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 1;
                cts[51] = new ClassToThreads(R12, ClassToThreads.SYNC, ids);

                return cts;
        }

        public static ClassToThreads[] getM2P4T4() {
                ClassToThreads[] cts = new ClassToThreads[22];

                // GR
                int[] ids = new int[4];
                ids[0] = 0;
                ids[1] = 1;
                ids[2] = 2;
                ids[3] = 3;
                cts[0] = new ClassToThreads(GR, ClassToThreads.SYNC, ids);

                // GW
                ids = new int[4];
                ids[0] = 0;
                ids[1] = 1;
                ids[2] = 2;
                ids[3] = 3;
                cts[1] = new ClassToThreads(GW, ClassToThreads.SYNC, ids);

                // R1
                ids = new int[1];
                ids[0] = 1;
                cts[2] = new ClassToThreads(R1, ClassToThreads.CONC, ids);

                // R2
                ids = new int[1];
                ids[0] = 2;
                cts[3] = new ClassToThreads(R2, ClassToThreads.CONC, ids);

                // R3
                ids = new int[1];
                ids[0] = 3;
                cts[4] = new ClassToThreads(R3, ClassToThreads.CONC, ids);

                // R4
                ids = new int[1];
                ids[0] = 0;
                cts[5] = new ClassToThreads(R4, ClassToThreads.CONC, ids);

                // W1
                ids = new int[1];
                ids[0] = 1;
                cts[6] = new ClassToThreads(W1, ClassToThreads.SYNC, ids);

                // W2
                ids = new int[1];
                ids[0] = 2;
                cts[7] = new ClassToThreads(W2, ClassToThreads.SYNC, ids);

                // W3
                ids = new int[1];
                ids[0] = 3;
                cts[8] = new ClassToThreads(W3, ClassToThreads.SYNC, ids);

                // W4
                ids = new int[1];
                ids[0] = 0;
                cts[9] = new ClassToThreads(W4, ClassToThreads.SYNC, ids);

                // W12
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 1;
                cts[10] = new ClassToThreads(W12, ClassToThreads.SYNC, ids);

                // W13
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 2;
                cts[11] = new ClassToThreads(W13, ClassToThreads.SYNC, ids);

                // W14
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 3;
                cts[12] = new ClassToThreads(W14, ClassToThreads.SYNC, ids);

                // W23
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 2;
                cts[13] = new ClassToThreads(W23, ClassToThreads.SYNC, ids);

                // W24
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 3;
                cts[14] = new ClassToThreads(W24, ClassToThreads.SYNC, ids);

                // W34
                ids = new int[2];
                ids[0] = 2;
                ids[1] = 3;
                cts[15] = new ClassToThreads(W34, ClassToThreads.SYNC, ids);

                // R12
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 1;
                cts[16] = new ClassToThreads(R12, ClassToThreads.SYNC, ids);

                // R13
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 2;
                cts[17] = new ClassToThreads(R13, ClassToThreads.SYNC, ids);

                // R14
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 3;
                cts[18] = new ClassToThreads(R14, ClassToThreads.SYNC, ids);

                // R23
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 2;
                cts[19] = new ClassToThreads(R23, ClassToThreads.SYNC, ids);

                // R24
                ids = new int[2];
                ids[0] = 1;
                ids[1] = 3;
                cts[20] = new ClassToThreads(R24, ClassToThreads.SYNC, ids);

                // R34
                ids = new int[2];
                ids[0] = 2;
                ids[1] = 3;
                cts[21] = new ClassToThreads(R34, ClassToThreads.SYNC, ids);

                return cts;

        }

        public static ClassToThreads[] getM2P2T2() {
                ClassToThreads[] cts = new ClassToThreads[6];

                // GR
                int[] ids = new int[2];
                ids[0] = 0;
                ids[1] = 1;
                cts[0] = new ClassToThreads(GR, ClassToThreads.SYNC, ids);

                // GW
                ids = new int[2];
                ids[0] = 0;
                ids[1] = 1;
                cts[1] = new ClassToThreads(GW, ClassToThreads.SYNC, ids);

                // R1
                ids = new int[1];
                ids[0] = 0;
                cts[2] = new ClassToThreads(R1, ClassToThreads.CONC, ids);

                // R2
                ids = new int[1];
                ids[0] = 1;
                cts[3] = new ClassToThreads(R2, ClassToThreads.CONC, ids);

                // W1
                ids = new int[1];
                ids[0] = 0;
                cts[4] = new ClassToThreads(W1, ClassToThreads.CONC, ids);

                // W2
                ids = new int[1];
                ids[0] = 1;
                cts[5] = new ClassToThreads(W2, ClassToThreads.CONC, ids);

                return cts;
        }

}

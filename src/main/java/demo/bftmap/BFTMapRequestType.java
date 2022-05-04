/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package demo.bftmap;

public class BFTMapRequestType {
        // comandos da aplicação
        public static final int TAB_CREATE = 1;
        public static final int TAB_REMOVE = 2;
        public static final int SIZE_TABLE = 3;
        public static final int PUT = 4;
        public static final int GET = 5;
        public static final int SIZE = 6;
        public static final int REMOVE = 7;
        public static final int CHECK = 8;
        public static final int GET_TABLE = 9;
        public static final int TAB_CREATE_CHECK = 10;
        public static final int EXIT = 11;
        public static final int PUT12 = 12;
        public static final int GET12 = 13;
        // comandos de checkpoint
        public static final int CKP = 14;
        public static final int CKPPART = 15;
        // comandos da recuperação
        public static final int RECOVERER = 16;
        public static final int SENDER = 17;
        public static final int METADATA = 18;
        public static final int STATE = 19;
        public static final int LOG = 20;
        public static final int LOG_RECOVERY = 21;
        public static final int RECOVERY_FINISHED = 22;

        public static String getOp(int id) {
                switch (id) {
                        case 1: return "TABLE_CREATE";
                        case 2: return "TAB_REMOVE";
                        case 3: return "SIZE_TABLE";
                        case 4: return "PUT";
                        case 5: return "GET";
                        case 6: return "SIZE";
                        case 7: return "REMOVE";
                        case 8: return "CHECK";
                        case 9: return "GET_TABLE";
                        case 10: return "TAB_CREATE_CHECK";
                        case 11: return "EXIT";
                        case 12: return "PUT12";
                        case 13: return "GET12";
                        case 14: return "CKP";
                        case 15: return "CKPPART";
                        case 16: return "RECOVERER";
                        case 17: return "SENDER";
                        case 18: return "METADATA";
                        case 19: return "STATE";
                        case 20: return "LOG";
                        case 21: return "LOG_RECOVERY";
                        case 22: return "RECOVERY_FINISHED";
                        default: return "NOT MAPPED";
                }
        }
}

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

}

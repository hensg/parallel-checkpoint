/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism;

import java.io.Serializable;

/**
 *
 * @author juninho
 */
public class Operation implements Serializable{
    public int operation;
    public int classID;
    public byte[] content;
    public int sequence;
    public Operation(int op,int classID, byte[] content, int sequence){
        this.operation=op;
        this.classID=classID;
        this.content=content;
        this.sequence=sequence;
    }
    public byte[] getContent(){
        return this.content;
    }
    public int getOperation(){
        return this.operation;
    }
    public int getClassId(){
        return this.classID;
    }
    public int getSequence(){
        return this.sequence;
    }
}

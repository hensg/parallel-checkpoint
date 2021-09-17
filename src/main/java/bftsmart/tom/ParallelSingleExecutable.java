/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom;
import bftsmart.tom.server.Executable;
import parallelism.MessageContextPair;
/**
 *
 * @author juninho
 */
public interface ParallelSingleExecutable extends Executable {

    public byte[] executeOrdered(byte[] bytes, MessageContextPair mc);
}
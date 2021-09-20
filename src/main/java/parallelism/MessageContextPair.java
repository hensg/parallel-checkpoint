/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism;

//import bftsmart.tom.MessageContext;
import bftsmart.tom.MessageContext;
import bftsmart.tom.core.messages.TOMMessage;
import java.io.Serializable;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 *
 * @author eduardo
 */
public class MessageContextPair implements Serializable, Cloneable {

    private static final Logger logger = LoggerFactory.getLogger(MessageContextPair.class);
    public TOMMessage request;
    public int classId;
    public byte[] operation;
    public MessageContext m;
    public int index;

    public byte[] resp;

    public MessageContextPair(TOMMessage message, int classId, int index, byte[] operation, MessageContext msgCtx) {
        this.request = message;
        this.classId = classId;
        this.index = index;
        this.operation = operation;
        this.m = msgCtx;
    }

    public MessageContextPair(MessageContextPair mcp) {
        this.request = mcp.request;
        this.classId = mcp.classId;
        this.index = mcp.index;
        this.operation = mcp.operation;
        this.m = mcp.m;
    }

    public MessageContextPair getClone() {
        try {
            // call clone in Object.
            return (MessageContextPair) super.clone();
        } catch (CloneNotSupportedException e) {
            logger.error("Cloning not allowed.", e.getCause());
            return this;
        }
    }

}

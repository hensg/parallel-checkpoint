package demo.bftmap;

import java.util.concurrent.atomic.AtomicInteger;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;

public class ReplyCounterListener implements ReplyListener{

    private AtomicInteger countReply = new AtomicInteger();

    @Override
    public void replyReceived(RequestContext arg0, TOMMessage arg1) {
        countReply.incrementAndGet();
    }

    public int getCountReply() {
        return countReply.get();
    }
}

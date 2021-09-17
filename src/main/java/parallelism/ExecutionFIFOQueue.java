/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parallelism;

/**
 *
 * @author eduardo
 */
public class ExecutionFIFOQueue<E> {

    /** Head of linked list */
    public FIFOQueue.Node<E> head = null;

    /** Tail of linked list */
    // public FIFOQueue.Node<E> last = null;

    public ExecutionFIFOQueue() {

    }

    public int getSize() {
        FIFOQueue.Node<E> atual = head;
        int c = 0;
        while (atual != null) {
            atual = atual.next;
            c++;
        }
        return c;
    }

    public E getNext() {
        return head.item;
    }

    public boolean goToNext() {
        head = head.next;
        return head != null;
    }

}

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.net.*;

/* server sender needs to check whether the message is for itself */
public class Server_Sender implements Runnable{
	// out records the messages to be sent, synchronized to be visited
	protected Queue<String> out_queue;
	
	// socket which communicates with server
	protected Socket comm;
	
	// lock associated with out queue;
	protected Object out_lock;
	
	/* transaction name set */
	protected Set<String> names;
	
	/* init lock */
	Object init_lock;
	
	/* init done */
	Boolean init;
	
	public Server_Sender(Socket comm, Queue<String> out, Object out_lock, Object init_lock, Boolean init, Set<String> names) {
		this.out_queue = out;
		this.comm = comm;
		this.out_lock = out_lock;
		this.init_lock = init_lock;
		this.init = init;
		this.names = names;
	}
	
	public void run() {
		try {
			PrintWriter output = new PrintWriter(comm.getOutputStream());

			// server sender block until receiver got init message
			synchronized(init_lock) {
				if (init.equals(Boolean.FALSE)) {
					init_lock.wait();
				}
				System.out.println("server sender got notification from receiver, send confirmation to client");

				output.println(Constant.initdone);
				output.flush();
			}
			
			
			// while loop forever, send message to server when queue is not empty
			while(true) {
				synchronized(out_lock) {
					// mutual exclusively visit out_queue
					// if the queue is not empty, send message to server
					if (!out_queue.isEmpty()) {
						String msg = out_queue.peek();
						if (checkmsg(msg)) {
							msg = out_queue.poll();
							output.println(msg);
							output.flush();	
						}
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}	
	
	// check message 
	public boolean checkmsg(String msg) {
		String[] msgs = msg.split(" ");
		if (names.contains(msgs[0])) {
			return true;
		}
		else return false;
	} 
}

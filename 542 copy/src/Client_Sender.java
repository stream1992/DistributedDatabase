import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;


public class Client_Sender implements Runnable{

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
	
	public Client_Sender(Socket comm, Queue<String> out, Object out_lock, Set<String> names, Object init_lock, Boolean init) {
		this.out_queue = out;
		this.comm = comm;
		this.out_lock = out_lock;
		this.names = names;
		this.init_lock = init_lock;
		this.init = init;
	}
	
	public void run() {
		try {
			PrintWriter output = new PrintWriter(comm.getOutputStream());
			// send name set to server, do initialization
			String msg = new String();
			Object[] name_set = names.toArray();
			if (name_set.length != Constant.transaction_n) {
				System.out.println(" name_set error ");
			}
			msg = (String)name_set[0];

			for (int i = 1; i < name_set.length; i ++) {
				msg = msg + " " + (String)name_set[i];

			}
			output.println(msg);
			output.flush();
			
			synchronized(init_lock) {
				if (init.equals(Boolean.FALSE)) {
					init_lock.wait();
				}
			}
			System.out.println(" Client sender init done");

			// while loop forever, send message to server when queue is not empty
			while(true) {
				synchronized(out_lock) {
					// mutual exclusively visit out_queue
					// if the queue is not empty, send message to server
					if (!out_queue.isEmpty()) {
						msg = out_queue.poll();
						output.println(msg);
						output.flush();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

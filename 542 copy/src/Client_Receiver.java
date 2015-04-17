import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Queue;
import java.util.Set;


public class Client_Receiver implements Runnable{

	// in_queue records the messages received from server, synchronized to be visited
	protected Queue<String> in_queue;
	
	// socket which communicates with server
	protected Socket comm;
	
	// lock associated with out queue;
	protected Object in_lock;
	
	/* transaction name set */
	protected Set<String> names;
	
	/* init lock */
	Object init_lock;
	
	/* init done */
	Boolean init;
	
	public Client_Receiver(Socket comm, Queue<String> in_queue, Object in_lock, Set<String> names,Object init_lock, Boolean init) {
		this.in_queue = in_queue;
		this.comm = comm;
		this.in_lock = in_lock;
		this.names = names;
		this.init_lock = init_lock;
		this.init = init;
	}
	
	public void run() {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(comm.getInputStream()));
			String line;
			// init with server, receive confimation
			line = in.readLine();
			if (line.equals(Constant.initdone)) {
				synchronized(init_lock) {
					init = Boolean.TRUE;
					init_lock.notifyAll();
				}
			}
			else {
				System.out.println("Server got wrong initial msg");
				throw new IOException();
			}
			System.out.println("Client receiver got init comfirmation");
			
			// the following will block there until got messages
			while ( (line = in.readLine()) != null ) {					
				synchronized(in_lock) {
					in_queue.offer(line);
					System.out.println("Client receiver got msg");

				}					
			}
		} catch (IOException e) {
			e.printStackTrace();
		}  
	}
}

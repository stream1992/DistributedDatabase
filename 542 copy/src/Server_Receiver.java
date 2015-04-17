import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Arrays;
import java.util.Queue;
import java.util.Set;


public class Server_Receiver implements Runnable{
	// in_queue records the messages received from server, synchronized to be visited
	protected Queue<String> in_queue;
	
	// socket which communicates with server
	protected Socket comm;
	
	// lock associated with out queue;
	protected Object in_lock;
	
	/* transaction name set */
	protected Set<String> names;
	
	/* cid set */
	Set<String> cids;
	
	/* cid set lock */
	Object cid_lock ;
	
	/* init lock */
	Object init_lock;
	
	/* init done */
	Boolean init;
	
	public Server_Receiver(Socket comm, Queue<String> in_queue, Object in_lock, Object init_lock, Boolean init, Set<String> names, Set<String> cids, Object cid_lock) {
		this.in_queue = in_queue;
		this.comm = comm;
		this.in_lock = in_lock;
		this.init_lock = init_lock;
		this.init = init;
		this.names = names;
		this.cid_lock = cid_lock;
		this.cids = cids;
	}
	public void run() {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(comm.getInputStream()));
			String line;
			
			line = in.readLine();
			String[] name_set = line.split(" ");
			if (name_set.length != Constant.transaction_n) {
				System.out.println("Server got wrong transaction numbers");
				throw new IOException();
			}
			for (int i = 0; i < name_set.length; i ++) {
				names.add(name_set[i]);
			}
			
			// register for this cliend in the server
			synchronized(cid_lock) {
				String cid = "";
				Arrays.sort(name_set);
				cid = cid + name_set[0];
				for (int i = 1; i < name_set.length; i ++) {
					cid = cid + "_" + name_set[i];
					System.out.println("Server add cid " + cid);
				}
				cids.add(cid);
			}
			
			// notify server_sender to continue
			synchronized(init_lock) {
				init = Boolean.TRUE;
				init_lock.notifyAll();
			}
			
			// the following will block there until got messages
			while ( (line = in.readLine()) != null ) {					
				synchronized(in_lock) {
					in_queue.offer(line);
				}					
			}

		} catch (IOException e) {
			e.printStackTrace();
		}  
	}
}

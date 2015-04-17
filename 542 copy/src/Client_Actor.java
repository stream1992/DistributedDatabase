import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.HashMap;
import java.util.Queue;

/* Client_Actor should know all the transaction locks, it gets msg from server and notify threads 
 * waiting for locks */
public class Client_Actor implements Runnable{

	// out records the messages to be sent, synchronized to be visited
		protected Queue<String> out_queue;
		// lock associated with out queue;
		protected Object out_lock;
		// in_queue records the messages received from server, synchronized to be visited
		protected Queue<String> in_queue;
		// lock associated with out queue;
		protected Object in_lock;
		// all the transaction lock table
		HashMap<String, Object> tran_lock_table;
		
		// Client id
		String cid;
		
		// hashtable: transaction item name with global item variable
		HashMap<String, Integer> globalmap;
		
		Live live1;
		Live live2;
		
		public Client_Actor(Queue<String> in_queue, Object in_lock, Queue<String> out_queue, Object out_lock, HashMap<String, Object> tran_lock_table, HashMap<String, Integer> globalmap, String cid, Live live1, Live live2) {
			this.in_queue = in_queue;
			this.in_lock = in_lock;
			this.tran_lock_table = tran_lock_table;
			this.globalmap = globalmap;
			this.out_queue = out_queue;
			this.out_lock = out_lock;
			this.cid = cid;
			this.live1 = live1;
			this.live2 = live2;
		}
		
		
		public void run() {
			String msg;
			while ( true ) {					
				synchronized(in_lock) {
					if (!in_queue.isEmpty()) {
						msg = in_queue.poll();
						String[] para = msg.split(" ");
						if (para[1].equals(Constant.lockgrant) || para[1].equals(Constant.commitdone)) {
							// notify thread waiting for this lock
							System.out.println("Client_Actor finds lock granted msg or commitdone");
							Object tran_lock = tran_lock_table.get(para[0]);
							synchronized(tran_lock) {
								tran_lock.notify();
							}
						}
						
						else if (para[1].equals(Constant.replication)) {
							// modify global values according to messages
							String[] commit_array = para[3].split("/");
							for (int i = 0; i < commit_array.length; i ++) {
								String[] pair = commit_array[i].split(",");
								if (!globalmap.containsKey(pair[0])) {
									System.out.println("commit msg error");
								}
								globalmap.put(pair[0], new Integer(pair[1]));
							}
							synchronized(out_lock) {
								out_queue.offer(Constant.build_commitreply(cid, para[2]));
							}
						}
						
						else if (para[1].equals(Constant.abort)) {
							// 
							System.out.println("Client_Actor finds abort msg for " + para[0]);
							String[] thread_name = cid.split("_");
							if (para[0].equals(thread_name[0])) {
								live1.die();;
								System.out.println("Client actor set live1 to false");
							}
							else {
								live2.die();;
								System.out.println("Client actor set live2 to false");

							}
							Object tran_lock = tran_lock_table.get(para[0]);
							synchronized(tran_lock) {
								tran_lock.notify();
							}
							
						}
						
					}
				}				
			}  
		}
}

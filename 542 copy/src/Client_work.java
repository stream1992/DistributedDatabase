import java.io.File;
import java.io.FileNotFoundException;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;


public class Client_work implements Runnable{
/* Client_work is a thread processing transactions, put get lock messages in the out_queue */
	
		// out records the messages to be sent, synchronized to be visited
		protected Queue<String> out_queue;
		// lock associated with out queue;
		protected Object out_lock;

		// Set to record the locks currenly holding
		
		// Thread transaction name
		String name;
		
		// Transation description file path
		String path;
				
		// Transaction lock
		Object tran_lock;
		
		// hashtable: transaction item name with local item variable
		HashMap<String, Integer> localmap;
		
		// hashtable: transaction item name with global item variable
		HashMap<String, Integer> globalmap;
		
		// read lock currently holding
		Set<String> readlock;
		// write lock currently holding
		Set<String> writelock;
		
		// live flag
		Live live;
		
		public Client_work(Queue<String> out_queue, Object out_lock, String transaction, Object tran_lock, HashMap<String, Integer> globalmap, Live live) {
			this.out_queue = out_queue;
			this.out_lock = out_lock;
			this.name = transaction;
			this.tran_lock = tran_lock;
			this.path = name;
			localmap = new HashMap<String, Integer>();
			this.globalmap = globalmap;
			readlock = new HashSet<String>();
			writelock = new HashSet<String>();
			this.live = live;
		}
		
		public void stop() {
			
		}
		
		public void run()  {
			synchronized(tran_lock) {
				File transaction_file = new File(path);
				try {
					Scanner scan = new Scanner(transaction_file);
					
					while (scan.hasNextLine()) {
						String[] operation = scan.nextLine().split(" ");
						if (operation[1].equals(Constant.w)) {
							// already got the write lock
							if (writelock.contains(operation[2])) {
								localmap.put(operation[2], Integer.valueOf(operation[3]));
							}
							// not lock
							else {
								// require lock from server
								String msg = Constant.build_getlock(name, "W", operation[2]);
								synchronized(out_lock) {
									out_queue.offer(msg);
								}
								tran_lock.wait();
								
								// transaction abort, just run out of the while loop
								if (live.isdead()) {
									System.out.println("Client work finds it is dead");
									break;
								}
								writelock.add(operation[2]);
								// got lock, do operation locally
								localmap.put(operation[2], Integer.valueOf(operation[3]));	
							}
							Thread.sleep(Constant.delay);

						}
						else if (operation[1].equals(Constant.r)) {
							if (writelock.contains(operation[2]) || readlock.contains(operation[2])) {
								localmap.put(operation[2], globalmap.get(operation[2]));
							}
							else {
								// require lock from server
								String msg = Constant.build_getlock(name, "R", operation[2]);
								synchronized(out_lock) {
									out_queue.offer(msg);
								}
								tran_lock.wait();
								
								// transaction abort, just run out of the while loop
								if (live.isdead()) {
									System.out.println("Client work finds it is dead");

									break;
								}
								// got lock, do operation locally
								readlock.add(operation[2]);

								localmap.put(operation[2], globalmap.get(operation[2]));	
							}
							Thread.sleep(Constant.delay);

						}
						else if (operation[1].equals(Constant.commit)) {
							// if the transaction holding its write lock, add it to commit set, change global values
							List<String> items = new LinkedList<String>();
							List<Integer> values = new LinkedList<Integer>();
							// change this client global values
							Iterator it = writelock.iterator();
							while (it.hasNext()) {
								String item = (String) it.next();
								items.add(item);
								values.add(localmap.get(item));
								
				//				globalmap.put(item, localmap.get(item));
							}
							String msg = Constant.build_commit(name, items, values);
							synchronized(out_lock) {
								out_queue.offer(msg);
							}
							// wait until commitment done
							tran_lock.wait();
							
						}
						else if (operation[1].equals(Constant.end)) {
							// release all locks got
							Iterator<String> r = readlock.iterator();
							Iterator<String> w = writelock.iterator();
							// if the read lock appears in the write set, it actually does not have it
							while (r.hasNext()) {
								String item = r.next();
								String msg = Constant.build_release(name, Constant.r, item);
								synchronized(out_lock) {
									out_queue.offer(msg);
								}
							}
							while (w.hasNext()) {
								String item = w.next();
								String msg = Constant.build_release(name, Constant.w, item);
								synchronized(out_lock) {
									out_queue.offer(msg);
								}
							}
							readlock.removeAll(readlock);
							writelock.removeAll(writelock);
						}
					}
					scan.close();
					if (live.isdead()) {
						System.out.println("Transaction abort " + name);
					}
					else {
						System.out.println("Transaction done from " + name);	
					}
					
				} catch (FileNotFoundException e1) {
					System.out.println("Error: Client_work did not find transaction file");
					e1.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			}
			
		}
}

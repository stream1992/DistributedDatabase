import java.net.*;
import java.io.*;
import java.util.*;
import java.util.logging.*;
public class Server_Actor implements Runnable{
/* Server_Actor is created when there is a connection, it keeps reading a message from in_queue
 * process it, and maybe 
 * put a message into the out_queue */
	
	// log file
	private final static Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	// in_queue records the messages received from client, synchronized to be visited
	protected Queue<String> in_queue;
	// lock associated with out queue;
	protected Object in_lock;
	
	protected Queue<String> out_queue;
	protected Object out_lock;

	// lock of X, global
	List<Lock_Entry> locktable;
	
	// cids registered on this server
	Set<String> cids;
	
	// transactions currently waiting for all commit reply
	HashMap<String, Set<String>> comm_table;
	
	// wait for graph, linked list
	HashMap<String, List<String>> graph;
	
	public Server_Actor(Queue<String> in_queue, Object in_lock, Queue<String> out_queue, Object out_lock, List<Lock_Entry> locktable, Set<String> cids) {
		this.in_queue = in_queue;
		this.in_lock = in_lock;
		this.out_queue = out_queue;
		this.out_lock = out_lock;
		this.locktable = locktable;
		this.cids = cids;
		comm_table = new HashMap<String, Set<String>>();
		graph = new HashMap<String, List<String>>();
	}
	
	public List<String> getvertex(String tran) {
		return graph.get(tran);
	}
	
	public void addedge(String tran1, String tran2) {
		if (tran1.equals(tran2)) {
			return;
		}
		if (!graph.containsKey(tran1)) {
			graph.put(tran1, new LinkedList<String>());
		}
		if (!graph.containsKey(tran2)) {
			graph.put(tran2, new LinkedList<String>());
		}
		List<String> v = graph.get(tran1);
		boolean flag = false;
		for (int i = 0; i < v.size(); i ++) {
			if (v.get(i).equals(tran2)) {
				flag = true;
			}
		}
		if (flag == false) {
			v.add(tran2);
		}
	}
	// BFS search start from tran
	public boolean deadlock(String start, String tran) {
		HashSet<String> set = new HashSet<String>();
		Queue<String> queue = new LinkedList<String>();
		queue.offer(tran);
		set.add(tran);
		while (!queue.isEmpty()) {
			String head = queue.poll();
			if (head.equals(start)) {
				return true;
			}
			List<String> v = graph.get(head);
			for (String vertex: v) {
				if (!set.contains(vertex)) {
					set.add(vertex);
					queue.offer(vertex);
				}
			}
		}
		return false;
	}
	public void clearup(String tran) {
		// delete elements in graph
		graph.remove(tran);
		Set<String> keyset = graph.keySet();
		Iterator<String> it = keyset.iterator();
		while (it.hasNext()) {
			List<String> values = graph.get(it.next());
			Iterator<String> it_values = values.iterator();
			while (it_values.hasNext()) {
				if (it_values.next().equals(tran)) {
					it_values.remove();
				}
			}
		}
		// delete entry in lock table
		for (Lock_Entry entry: locktable) {
			Iterator<Waitentry> it_w = entry.waitlist().iterator();
			while (it_w.hasNext()) {
				if (it_w.next().tran.equals(tran)) {
					it_w.remove();
				}
			}
			Iterator<Waitentry> it_h = entry.current.iterator();
			while (it_h.hasNext()) {
				if (it_h.next().tran.equals(tran)) {
					it_h.remove();
				}
			}
			
			// if the item is free now, release lock to waiting transactions
			// check is there waiting transactions
			if (entry.isWait()) {
				// no transaction is locking the item now
				List<Waitentry> waitlist = entry.waitlist();		// *************************
				
				if (!entry.isLock()) {
					// reply to client, get lock
					int idx = 0;
					while (waitlist.size() > 0 && waitlist.get(idx).isRead()) {
						Waitentry waitentry = waitlist.get(idx);
						entry.getlock(waitentry);
						synchronized(out_lock) {
							out_queue.offer(waitentry.tran + " " + Constant.lockgrant);	
							log.info("Clear up :Server release R lock for " + waitentry.tran + " " + entry.item);
						}
						boolean f = true;
						while (f == true) {
							f = false;
							// add edges, check deadlock
							for (int j = 0; j < waitlist.size(); j ++) {
								Waitentry w = waitlist.get(j);
								if (w.isWrite()) {
									// add edge
									addedge(waitentry.tran, w.tran);
									
									if (deadlock(waitentry.tran, w.tran)) {
										f = true;
										clearup(w.tran);
										log.info("Clear up: Server detect deadlock during releasing, abort the transaction");
										synchronized(out_lock) {
											out_queue.offer(w.tran + " " + Constant.abort);
											log.info("Clear up: Server sent abort command to " + w.tran);
										}
										break;
									}
								}
							}
						}
						
					}
					List<String> holding = entry.holding_tran();
					if (holding.size() == 1 && entry.isReadLock() && entry.isWait()) {
						List<Waitentry> w = entry.waitlist();
						boolean flag = false;
						Waitentry add = null;
						for (int i = 0; i < w.size(); i ++) {
							if (w.get(i).tran.equals(holding.get(0))) {
								flag = true;
								add = w.get(i);
								w.remove(i);
								break;
							}
						}
						if (flag == true) {
							entry.current.add(add);	
							synchronized(out_lock) {
								out_queue.offer(add.tran + " " + Constant.lockgrant);	
								log.info("Clear up: Server release " + add.op + " lock for " + add.tran + " " + entry.item);
							}
						}
					}
					// no read lock add, just add the first w lock
					if (entry.isWait() && holding.size() == 0) {
						Waitentry add = waitlist.get(0);
						entry.waitlist().remove(0);
						entry.current.add(add);
						synchronized(out_lock) {
							out_queue.offer(add.tran + " " + Constant.lockgrant);	
							log.info("Server release " + add.op + " lock for " + add.tran + " " + entry.item);
						}
						// add edges, check deadlock
						boolean f = true;
						while (f == true) {
							f = false;
							for (int j = 0; j < waitlist.size(); j ++) {
								Waitentry w = waitlist.get(j);
								if (w.isWrite()) {
									// add edge
									addedge(add.tran, w.tran);
									if (deadlock(add.tran, w.tran)) {
										f = true;
										clearup(w.tran);
										log.info("Server detect deadlock during releasing, abort the transaction");
										synchronized(out_lock) {
											out_queue.offer(w.tran + " " + Constant.abort);
											log.info("Server sent abort command to " + w.tran);
										}
										break;
									}
								}
							}
						}
					}
				}
				// current lock is read lock, check whether the first waiting is compatible
				else if (entry.isReadLock()) {
					List<String> holding = entry.holding_tran();
					
					if (holding.size() == 1 && entry.isReadLock() && entry.isWait()) {
						List<Waitentry> w = entry.waitlist();
						boolean flag = false;
						Waitentry add = null;
						for (int i = 0; i < w.size(); i ++) {
							if (w.get(i).tran.equals(holding.get(0))) {
								flag = true;
								add = w.get(i);
								w.remove(i);
								break;
							}
						}
						if (flag == true) {
							entry.current.add(add);	
							synchronized(out_lock) {
								out_queue.offer(add.tran + " " + Constant.lockgrant);	
								log.info("Clear up: Server release " + add.op + " lock for " + add.tran + " " + entry.item);
							}
						}
					}
				}
			}
		}
	}
	@Override
	public void run() {
		while (true) {
			synchronized(in_lock) {

				if (!in_queue.isEmpty()) {
					String msg = in_queue.poll();
					String[] para = msg.split(" ");
					
					String op = para[1];
					
					if (op.equals(Constant.getlock)) {
						String tran = para[0];
						String addition = para[2];
						String target = para[3];
						log.info("Server got lock request from " + tran + " " + "for " + target);
						synchronized(locktable) {
							// search locktable, if exists, add to wait list, if not, add lock entry
							boolean exist = false;
							for (int i = 0; i < locktable.size(); i ++) {
								Lock_Entry lock_entry = locktable.get(i);
								String item = lock_entry.getitem();
								if (item.equals(target)) {
									exist = true;
									// check whether can get the lock
									
									// the item is free
									if (!lock_entry.isLock()) {
										lock_entry.getlock(new Waitentry(tran, addition));
										// reply client, get lock
										synchronized(out_lock) {
											out_queue.offer(tran + " " + Constant.lockgrant);	
											log.info("Server release " + para[2] + " lock for " + tran + " " + item);
										}
										break;
									}
									// the item has been locked
									else {
										List<String> holding = lock_entry.holding_tran();
										String first = holding.get(0);
										
										// compatible read lock
										if (addition.equals(Constant.r) && lock_entry.isReadLock()) {
											lock_entry.getlock(new Waitentry(tran, addition));
											// add edges and check deadlock
											List<Waitentry> waitlist = lock_entry.waitlist();
											boolean f = true;
											while (f == true) {
												f = false;
												for (int j = 0; j < waitlist.size(); j ++) {
													if (waitlist.get(j).isWrite() && (!waitlist.get(j).tran.equals(tran))) {
														addedge(tran, waitlist.get(j).tran);
														if (deadlock(tran, waitlist.get(j).tran)) {
															f = true;
															clearup(waitlist.get(j).tran);
															synchronized(out_lock) {
																out_queue.offer(waitlist.get(j).tran + " " + Constant.abort);	
																log.info("Server detect deadlock when releasing read lock for " + waitlist.get(j).tran + " on " + item);
															}
															break;
														}
													}
												}
											}
											
											synchronized(out_lock) {
												out_queue.offer(tran + " " + Constant.lockgrant);	
												log.info("Server release compatible read lock for " + tran + " on " + item);
											}	
											
										}
										// the write lock is from the same transaction holding read lock
										else if (holding.size() == 1 && first.equals(tran) && addition.equals(Constant.w)){
											// release read lock
										//	lock_entry.release(tran);
											// put on write lock
											lock_entry.getlock(new Waitentry(tran, op));
											synchronized(out_lock) {
												out_queue.offer(tran + " " + Constant.lockgrant);	
												log.info("Server change read lock for " + tran + " to write lock on " + item);
											}
										}
										// put on the wait list
										else {
											/* add edge */
											boolean detect = false;
											if (addition.equals(Constant.w) ) {
												for (int j = 0 ; j < holding.size(); j ++) {
													if (holding.get(j).equals(tran)) {
														continue;
													}
													addedge(holding.get(j), tran);
													// detect deadlock
													if (deadlock(holding.get(j), tran)) {
														log.info("Server detect deadlock, abort the transaction");
														clearup(tran);
														synchronized(out_lock) {
															out_queue.offer(tran + " " + Constant.abort);
															log.info("Server sent abort command to " + tran);
														}
														detect = true;
														break;
													}
												}
												// no deadlock
												if (detect == false) {
													lock_entry.wait(new Waitentry(tran, addition));
													log.info("Server put " + para[2] + " lock for " + item + " from " + tran + " wait list");	
												}
												
											}
											
											else {
												lock_entry.wait(new Waitentry(tran, addition));
												log.info("Server put " + para[2] + " lock for " + item + " from " + tran + " wait list");
											}
											
										}
									}
									
								}
							}
							// did not find the item, new a lock entry
							if (exist == false) {
								Lock_Entry newitem = new Lock_Entry(target);
								locktable.add(newitem);
								newitem.getlock(new Waitentry(tran, addition));
								// reply client
								synchronized(out_lock) {
				//					System.out.println("Server_Actor release lock");	
									out_queue.offer(tran + " " + Constant.lockgrant);	
									log.info("Server release " + para[2] + " lock for " + tran + " " + target);
								}
							}
						}
						
					}
					else if (op.equals(Constant.releaselock)) {
						String tran = para[0];
						String target = para[3];
						String type = para[2];
						log.info("Server finds release " + type + " lock request from " + tran+ " for " + target);
						synchronized(locktable) {
							Lock_Entry lock_entry = null;
							for (int i = 0; i < locktable.size(); i ++) {
								if (locktable.get(i).getitem().equals(target)) {
									lock_entry = locktable.get(i);
									break;
								}
							}
							if (lock_entry == null) {
								System.out.println("Error: Got a release lock msg, but lock item do not exist");
							}
							// release lock
							lock_entry.release(tran, type);
							
							// check is there waiting transactions
							if (lock_entry.isWait()) {
								// no transaction is locking the item now
								List<Waitentry> waitlist = lock_entry.waitlist();		
								
								if (!lock_entry.isLock()) {
									// reply to client, get lock
									int idx = 0;
									while (waitlist.size() > 0 && waitlist.get(idx).isRead()) {
										Waitentry waitentry = waitlist.get(idx);
										lock_entry.getlock(waitentry);
										synchronized(out_lock) {
											out_queue.offer(waitentry.tran + " " + Constant.lockgrant);	
											log.info("Server release " + waitentry.op + " lock for " + waitentry.tran + " " + lock_entry.item);
										}
										// add edges, check deadlock
										boolean f = true;
										while (f == true) {
											f = false;
											for (int j = 0; j < waitlist.size(); j ++) {
												Waitentry w = waitlist.get(j);
												if (w.isWrite()) {
													// add edge
													addedge(waitentry.tran, w.tran);
													if (deadlock(waitentry.tran, w.tran)) {
														f = true;
														clearup(w.tran);
														log.info("Server detect deadlock during releasing, abort the transaction");
														synchronized(out_lock) {
															out_queue.offer(w.tran + " " + Constant.abort);
															log.info("Server sent abort command to " + w.tran);
														}
														break;
													}
												}
											}
										}
										
									}
									List<String> holding = lock_entry.holding_tran();
									if (lock_entry.isWait() && holding.size() == 1 && lock_entry.isReadLock()) {
										List<Waitentry> w = lock_entry.waitlist();
										boolean flag = false;
										Waitentry add = null;
										for (int i = 0; i < w.size(); i ++) {
											if (w.get(i).tran.equals(holding.get(0))) {
												flag = true;
												add = w.get(i);
												w.remove(i);
												break;
											}
										}
										if (flag == true) {
											lock_entry.current.add(add);	
											synchronized(out_lock) {
												out_queue.offer(add.tran + " " + Constant.lockgrant);	
												log.info("Server release " + add.op + " lock for " + add.tran + " " + lock_entry.item);
											}
										}
										
									}
									// no read lock add, just add the first w lock
									if (lock_entry.isWait() && holding.size() == 0) {
										Waitentry add = waitlist.get(0);
										lock_entry.waitlist().remove(0);
										lock_entry.current.add(add);
										synchronized(out_lock) {
											out_queue.offer(add.tran + " " + Constant.lockgrant);	
											log.info("Server release " + add.op + " lock for " + add.tran + " " + lock_entry.item);
										}
										// add edges, check deadlock
										boolean f = true;
										while (f == true) {
											f = false;
											for (int j = 0; j < waitlist.size(); j ++) {
												Waitentry w = waitlist.get(j);
												if (w.isWrite()) {
													// add edge
													addedge(add.tran, w.tran);
													if (deadlock(add.tran, w.tran)) {
														f = true;
														clearup(w.tran);
														log.info("Server detect deadlock during releasing, abort the transaction");
														synchronized(out_lock) {
															out_queue.offer(w.tran + " " + Constant.abort);
															log.info("Server sent abort command to " + w.tran);
														}
														break;
													}
												}
											}
										}
									} 
								}
								
								// current lock is read lock, check whether the first waiting is compatible
								else if (lock_entry.isReadLock()) {
									List<String> holding = lock_entry.holding_tran();
									String curr = holding.get(0);
									if (holding.size() == 1) {
										List<Waitentry> w = lock_entry.waitlist();
										boolean flag = false;
										Waitentry add = null;
										for (int i = 0; i < w.size(); i ++) {
											if (w.get(i).tran.equals(holding.get(0))) {
												flag = true;
												add = w.get(i);
												w.remove(i);
												break;
											}
										}
										if (flag == true) {
											lock_entry.current.add(add);	
											synchronized(out_lock) {
												out_queue.offer(add.tran + " " + Constant.lockgrant);	
												log.info("Server release " + add.op + " lock for " + add.tran + " " + lock_entry.item);
											}
										}
									}
								}
							}
							
						}
					}
					
					else if (op.equals(Constant.commit)) {
						// send all values to other clients
						String comm_tran = para[0];
						comm_table.put(comm_tran, new HashSet<String>(cids));
						
						log.info("Server got commit message from " + comm_tran);
						Iterator it = cids.iterator();
						while (it.hasNext()) {
							String cid = (String) it.next();
							String[] tran_names = cid.split("_");
							String comm_msg = Constant.build_replication(tran_names[0], comm_tran, para[2]);
							synchronized(out_lock) {
								out_queue.offer(comm_msg);	
							}
							log.info("Server sent replication request to "+ tran_names[0] + " to copy values from " + comm_tran);
						}
					}
					else if (op.equals(Constant.commitreply)) {
						String comm_tran = para[2];
						String cid = para[0];
						if (!comm_table.containsKey(comm_tran)) {
							System.out.println("commitreply can not find in comm_table");
						}
						Set<String> waiting = comm_table.get(comm_tran);
						waiting.remove(cid);
						log.info("Server got commitreply from client " + cid + " for " + comm_tran);
						
						// all clients have replied, send commitdone msg
						if (waiting.isEmpty()) {
							synchronized(out_lock) {
								out_queue.offer(Constant.build_commitdone(comm_tran));
							}
							log.info("Server sent commitdone to "+ comm_tran);	
						}
						else {
						}
					}
					
				}
			} 
		}
	}
}

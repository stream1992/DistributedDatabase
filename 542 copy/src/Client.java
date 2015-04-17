import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
public class Client {
/* All participate sites run this, contains income message queue, output message queue
 * a communication thread, read messages from server and put it in income queue, 
 * send messages from output queue.
 * an actor thread, process messages from income queue, notify threads waiting for locks
 * and many threads processing transactions  */
	
	/* income message queue, sharing in all client threads */
	protected Queue<String> in_queue;
	protected Object in_lock;
	/* output message queue, sharing in all client threads */
	protected Queue<String> out_queue;
	protected Object out_lock;
	
	Socket comm;

	/* transaction lock table, associated with Client cid and work thread number */
	HashMap<String, Object> tran_lock_table;

	Object tran_lock1;
	Object tran_lock2;
	
	/* transaction name set */
	Set<String> names;
	
	/* Client id */
	String cid;
	
	/* Runnable Client Actor */
	Client_Actor client_actor;
	
	/* Runnable Client sender */
	Client_Sender client_sender;
	
	/* Runnable Client receiver */
	Client_Receiver client_receiver;
	
	/* Runnable Client_work */
	Client_work client_work1;
	Client_work client_work2;

	/* init lock */
	Object init_lock;
	
	/* init done */
	Boolean init;
	
	// hashtable: transaction item name with global item variable
	HashMap<String, Integer> globalmap;
	
	// Live flag indicating live or not
	Live live1;
	Live live2;

	
	public Client(String transaction1, String transaction2) {
		// initiate queues for this client
		in_queue = new LinkedList<String>();
		in_lock = new Object();
		out_queue = new LinkedList<String>();
		out_lock = new Object();
		
		// set names
		names = new HashSet<String>();
		names.add(transaction1);
		names.add(transaction2);

		// set client id
		cid = transaction1 + "_" + transaction2;
		
		// set lock associated with the name
		tran_lock1 = new Object();
		tran_lock2 = new Object();
		
		// set tran_lock_table
		tran_lock_table = new HashMap<String, Object>();
		tran_lock_table.put(transaction1, tran_lock1);
		tran_lock_table.put(transaction2, tran_lock2);
		
		// init lock
		init_lock = new Object();
		init = Boolean.valueOf(false);
		
		// Init global values
		globalmap = new HashMap<String, Integer>();
		globalmap.put(Constant.A, new Integer(1));
		globalmap.put(Constant.B, new Integer(4));
		globalmap.put(Constant.X, new Integer(7));
		globalmap.put(Constant.Y, new Integer(10));
		
		//initiate two work threads alive
		live1 = new Live(true);
		live2 = new Live(true);
		
		// initiate communication with server
		try {
			InetAddress add = InetAddress.getLocalHost();
			comm = new Socket(add, Server.portn);
			System.out.println("Client got connection");
			// new a sender
			client_sender = new Client_Sender(comm, out_queue, out_lock, names, init_lock, init);
			// new a receiver
			client_receiver = new Client_Receiver(comm, in_queue, in_lock, names, init_lock, init);
			// new an actor
			client_actor = new Client_Actor(in_queue, in_lock, out_queue, out_lock, tran_lock_table, globalmap, cid, live1, live2);
			// new two workers
			client_work1 = new Client_work(out_queue, out_lock, transaction1, tran_lock1, globalmap, live1);
			client_work2 = new Client_work(out_queue, out_lock, transaction2, tran_lock2, globalmap, live2);
			
			// start all the threads
			new Thread(client_sender).start();
			new Thread(client_receiver).start();
			new Thread(client_actor).start();
			new Thread(client_work1).start();
			new Thread(client_work2).start();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public static void main(String[] args) {
		Client client = new Client(args[0], args[1]);
	}
}

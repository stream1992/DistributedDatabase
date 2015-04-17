import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
/*Server initiate a Server_Actor for each client, and a Server_sender, a Server_receiver thread, 
 * an in_queue msg, a out_queue msg
 * */
public class Server {
	/*port number*/
	public static final int portn = 9998;
	
	// Allocate in_queue, out_queue, seen by all sender and receiver. Global
	Queue<String> in_queue;
	Object in_lock;
	Queue<String> out_queue;
	Object out_lock;
	
	/* lock table */
	List<Lock_Entry> locktable;

	/* Server_Actor thread */
	Server_Actor server_actor;
	
	/*listen server socket*/
	ServerSocket listen;
	
	/* cid set */
	Set<String> cid;
	
	/* cid lock */
	Object cid_lock;
	
	public Server() {
		locktable = new LinkedList<Lock_Entry>();
		
		in_queue = new LinkedList<String>();
		in_lock = new Object();
		out_queue = new LinkedList<String>();
		out_lock = new Object();
		
		cid = new HashSet<String>();
		cid_lock = new Object();
		
		server_actor = new Server_Actor(in_queue, in_lock, out_queue, out_lock, locktable, cid);
		// start server_actor
		new Thread(server_actor).start();
	}
	
	public static void main(String[] args) {
		Server server = new Server();
		try {
			server.listen = new ServerSocket(portn);
			while (true) {
				System.out.println("Server is waiting for connecting");
				Socket incoming = server.listen.accept();
				System.out.println("Server got a connection");
				
				// allocate init lock
				Object init_lock = new Object();
				Boolean init = Boolean.valueOf(false);
				
				// name set
				Set<String> names = new HashSet<String>();
				
				// Runnable sender and receiver
				Server_Sender sender = new Server_Sender(incoming,server.out_queue,server.out_lock,init_lock,init,names);
				Server_Receiver rece = new Server_Receiver(incoming,server.in_queue,server.in_lock,init_lock,init,names,server.cid, server.cid_lock);
				
				// a sender thread to send messages to client
				new Thread(sender).start();
				// a receiver thread get messages from client
				new Thread(rece).start();
			}
		}catch(IOException ex) {
			ex.printStackTrace();
		}
	}
}

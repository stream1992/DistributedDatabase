import java.awt.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class DistributedDatabase {

	public static void main(String[] args) throws InterruptedException {
		
		ProcessBuilder pbserver = new ProcessBuilder("java", "Server");
		ProcessBuilder pbclient1 = new ProcessBuilder("java", "Client", "T1", "T2");
		ProcessBuilder pbclient2 = new ProcessBuilder("java", "Client", "T3", "T4");

		try {
			Process p = pbserver.start();
			Process c1 = pbclient1.start();
			Process c2 = pbclient2.start();
			InputStream stdout = p.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
			String line = reader.readLine();
			while (line != null  && ! line.trim().equals("--EOF--")) {
				System.out.println("Server output " + line);
				line = reader.readLine();
			} 
			
			InputStream stdout1 = c1.getInputStream();
			BufferedReader reader1 = new BufferedReader(new InputStreamReader(stdout1));
			String line1 = reader1.readLine();
			while (line1 != null  && ! line.trim().equals("--EOF--")) {
				System.out.println("Client1 output " + line1);
				line1 = reader1.readLine();
			}
			
			InputStream stdout2 = c2.getInputStream();
			BufferedReader reader2 = new BufferedReader(new InputStreamReader(stdout2));
			String line2 = reader2.readLine();
			while (line2 != null  && ! line.trim().equals("--EOF--")) {
				System.out.println("Client2 output " + line2);
				line2 = reader2.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
/*	
	private static class IOThreadHandler extends Thread {
		private InputStream inputStream;
		
	}
*/
}

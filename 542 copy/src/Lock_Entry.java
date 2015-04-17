import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

class Waitentry {
	String tran;
	String op;
	
	public Waitentry(String tran, String op) {
		this.tran = tran;
		this.op = op;
	}
	
	public boolean isRead() {
		return op.equals(Constant.r);
	}
	
	public boolean isWrite() {
		return op.equals(Constant.w);
	}
	
	public String getTran() {
		return tran;
	}
}
public class Lock_Entry {

	String item;
	List<Waitentry> current;

	List<Waitentry> wait;
	
	public Lock_Entry(String item) {
		this.item = item;
		this.current = new LinkedList<Waitentry>();
		this.wait = new LinkedList<Waitentry>();
	}
	public String getitem() {
		return item;
	}
	public boolean isLock() {
		if (current.isEmpty()) {
			return false;
		}
		else return true;
	}
	public boolean isReadLock() {
		Waitentry en = current.get(0);
		if (en.isRead()) {
			return true;
		}
		else return false;
	}
	public void wait(Waitentry en) {
		wait.add(en);
	}
	public void release(String tran, String type) {
		for (int i = 0; i < current.size(); i ++) {
			if (current.get(i).tran.equals(tran) && current.get(i).op.equals(type)) {
				System.out.println("Lock_Entry realease " + type + " lock from " + tran);
				current.remove(i);
			}
		}
	}
	public List<String> holding_tran() {
		List<String> res = new ArrayList<String>();
		if (isLock() == false) {
			return res;
		}
		for (int i = 0; i < current.size(); i ++) {
			res.add(current.get(i).tran);
		}
		return res;
	}
	
	public boolean isWait() {
		return !(wait.isEmpty());
	}
	
	public Waitentry peekfirstwait() {
		return wait.get(0);
	}
	
	public void getlock(Waitentry en) {
		if (wait.size() > 0) {
			Waitentry first = wait.get(0);
			if (first.op.equals(en.op) && first.tran.equals(en.tran)) {
				wait.remove(0);
			}	
		}
		current.add(en);
	}
	public List<Waitentry> waitlist() {
		return wait;
	}
}

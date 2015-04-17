
public class Live {

	boolean flag;
	public void die() {
		flag = false;
	}
	public boolean isdead() {
		return (!flag);
	}
	public Live (boolean flag) {
		this.flag = flag;
	} 
}

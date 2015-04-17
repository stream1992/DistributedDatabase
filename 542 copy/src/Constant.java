import java.util.List;


public class Constant {

	public static final int delay = 3000;
	
	public static final String X = "X";
	public static final String Y = "Y";
	public static final String A = "A";
	public static final String B = "B";
	
	public static final String T1 = "T1";
	public static final String T2 = "T2";
	public static final String T3 = "T3";
	public static final String T4 = "T4";

	public static final String getlock = "getlock";
	public static final String releaselock = "releaselock";
	
	public static final String lockgrant = "lockgrant";
	public static final String abort = "abort";
	public static final String commit = "commit";
	public static final String commitdone = "commitdone";
	public static final String commitreply = "commitreply";
	public static final String replication = "replication";

	
	
	public static final String end = "end";

	
	
	public static final String w = "W";
	public static final String r = "R";
	
	public static final int transaction_n = 2;
	
	public static final String initdone = "initdone";
	
	/* messgaes from client contain four parts */
	public static String build_getlock(String name, String type, String item) {
		String msg;
		msg = name + " " + Constant.getlock + " " + type + " " + item;
		return msg;
	}
	public static String build_release(String name, String type, String item) {
		String msg;
		msg = name + " " + Constant.releaselock + " " + type + " " + item;
		return msg;
	}
	
	/*eg: T3 commit X,1/Y,2  */
	public static String build_commit(String name, List<String> items, List<Integer> values) {
		String msg = "";
		msg = msg + name + " " + Constant.commit + " ";
		for (int i = 0; i < items.size() - 1; i ++) {
			msg = msg + items.get(i) + "," + values.get(i) + "/";
		}
		msg = msg + items.get(items.size() - 1) + "," + values.get(values.size() - 1);
		return msg;
	}
	
	/*eg: T1_T2 commitreply T3*/
	public static String build_commitreply(String cid, String tran) {
		String msg = "";
		msg = msg + cid + " " + Constant.commitreply + " " + tran;
		return msg;
	}
	/* eg: T1 replication T3 X,1/Y,2 , T1 is changing values to T3*/
	public static String build_replication(String name, String tran, String content) {
		String msg = "";
		msg = msg + name + " " + Constant.replication + " " + tran + " " + content;
		return msg;
	}
	public static String build_commitdone(String name) {
		String msg = "";
		msg = msg + name + " " + Constant.commitdone;
		return msg;
	}
}

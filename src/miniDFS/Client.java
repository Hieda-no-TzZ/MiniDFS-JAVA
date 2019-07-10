package miniDFS;

import java.util.LinkedList;

public class Client {

	public static void main(String[] args) {
		NameServer ns = new NameServer();
		DataServer ds1 = new DataServer("node1");
		DataServer ds2 = new DataServer("node2");
		DataServer ds3 = new DataServer("node3");
		DataServer ds4 = new DataServer("node4");
		ns.addDS(ds1);
		ns.addDS(ds2);
		ns.addDS(ds3);
		ns.addDS(ds4);
		for (DataServer ds : ns.dataservers) {
			ds.start();
		}
		ns.start();
	}

}

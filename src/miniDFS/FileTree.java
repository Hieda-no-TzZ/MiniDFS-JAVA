package miniDFS;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class FileTree {
	Node root = new Node("/");

	public FileTree() {
		// TODO Auto-generated constructor stub
	}

	public LinkedList<String> parsePath(String path) {
		LinkedList<String> paths = new LinkedList<>();
		for (String s : path.split("/")) {
			if (s.equals(""))
				continue;
			paths.add(s);
		}
		return paths;
	}

	public void addDir(String path) {
		LinkedList<String> paths = parsePath(path);
		int n = paths.size();
		Node curnode = root;
		for (int i = 0; i < n; ++i) {
			String dirName = paths.get(i);
			if (!curnode.children.containsKey(dirName)) { // 不存在，直接创建
				curnode.addChild(new Node(dirName));
			} else { // 存在
				if (curnode.children.get(dirName).fileID != -1) { // 是文件，报错
					for (int j = 0; j <= i; ++j) {
						System.out.print("/" + paths.get(j));
					}
					System.out.println(" is a file but not a dir.");
					return;
				}
			}
			curnode = curnode.children.get(dirName);
		}
	}

	public void del(String path) {
		LinkedList<String> paths = parsePath(path);
		int n = paths.size();
		Node curnode = root;
		String dirName;
		for (int i = 0; i < n - 1; ++i) {
			dirName = paths.get(i);
			if (!curnode.children.containsKey(dirName)) {
				for (int j = 0; j <= i; ++j) {
					System.out.print("/" + paths.get(j));
				}
				System.out.println("dir not exists.");
			} else {
				curnode = curnode.children.get(dirName);
			}
		}
		dirName = paths.get(n - 1);
		if (!curnode.children.containsKey(dirName)) {
			System.out.println(paths + " not exists.");
		} else {
			curnode.children.remove(dirName);
		}
	}

	public int addFile(String path, int fileID) {
		LinkedList<String> paths = parsePath(path);
		int n = paths.size();
		Node curnode = root;
		for (int i = 0; i < n; ++i) {
			String dirName = paths.get(i);
			if (!curnode.children.containsKey(dirName)) { // 不存在，则可以创建
				curnode.addChild(new Node(dirName));
			} else { // 存在
				if (curnode.children.get(dirName).fileID != -1 && i != n - 1) { // 如果是文件，并且不是最后一个，报错
					for (int j = 0; j <= i; ++j) {
						System.out.print("/" + paths.get(j));
					}
					System.out.println(" is a file but not a dir.");
					return -1;
				} else if (i == n - 1) { // 如果不是文件，并且是最后一个，则报错
					for (int j = 0; j <= i; ++j) {
						System.out.print("/" + paths.get(j));
					}
					System.out.println(" is a dir but not a file.");
					return -1;
				}
			}
			curnode = curnode.children.get(dirName);
		}
		curnode.fileID = fileID;
		return fileID;
	}

	public void display() {
		Node curnode = this.root;
		Queue<Node> queue = new LinkedList<Node>();
		queue.offer(curnode);
		while (!queue.isEmpty()) {
			Node node = queue.poll();
			System.out.println(node.name + " " + node.fileID);
			for (Node n : node.children.values()) {
				queue.offer(n);
			}
		}
	}

	public void display(Node node) {
		for (Node n : node.children.values()) {
			if (n.fileID == -1) {
				System.out.println(n.name + "/");
			} else {
				System.out.println(n.name + "\t" + n.fileID);
			}
		}
	}

	public void display(LinkedList<String> path) {
		Node curnode = this.root;
		for (int i = 0; i < path.size(); ++i) {
			curnode = curnode.children.get(path.get(i));
		}
		display(curnode);
	}

	public boolean existDir(LinkedList<String> path, String dir) {
		Node curnode = this.root;
		for (int i = 0; i < path.size(); ++i) {
			if (!curnode.children.containsKey(path.get(i)))
				return false;
			curnode = curnode.children.get(path.get(i));
		}
		if (curnode.children.containsKey(dir)) {
			if (curnode.children.get(dir).fileID != -1) {
				return false;
			}
			return true;
		} else {
			return false;
		}
	}

	public boolean existFile(String treepath) {
		LinkedList<String> path = parsePath(treepath);
		Node curnode = this.root;
		for (int i = 0; i < path.size(); ++i) {
			if (!curnode.children.containsKey(path.get(i)))
				return false;
			curnode = curnode.children.get(path.get(i));
		}
		if (curnode.fileID == -1) {
			return false;
		} else {
			return true;
		}
	}
	
	public int getFileId(String treepath) {
		LinkedList<String> path = parsePath(treepath);
		Node curnode = this.root;
		for (int i = 0; i < path.size(); ++i) {
			if (!curnode.children.containsKey(path.get(i)))
				return -1;
			curnode = curnode.children.get(path.get(i));
		}
		return curnode.fileID;
	}

	class Node {
		String name;
		HashMap<String, Node> children = new HashMap<>();
		int fileID = -1;

		Node(String name) {
			this.name = name;
		}

		public void addChild(Node child) {
			this.children.put(child.name, child);
		}
	}
}
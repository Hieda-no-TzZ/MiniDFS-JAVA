package miniDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

public class NameServer extends Thread {
	public LinkedList<DataServer> dataservers = new LinkedList<>();
	public final int chunkSize = 2 * 1024 * 1024;
	public static int fileIDgenerater = 0;
	public static FileTree fileTree;
	public static HashMap<Integer, LinkedList<String>> md5List = new HashMap<>();
	public static HashMap<Integer, Integer> chunkNum = new HashMap<>();
	public static HashMap<Integer, int[]> dsList = new HashMap<>();
	public final int repNum = 3;
	
	NameServer() {
		fileTree = new FileTree();
	}

	@Override
	public void run() {
		LinkedList<String> pwd = new LinkedList<>(); // 当前目录
		Scanner in = new Scanner(System.in);
		String cmd = "";
		while (true) {
			System.out.print("miniDFS: " + mergePath(pwd) + "$ ");
			cmd = StringUtils.strip(in.nextLine());
			if (cmd.equals("exit") || cmd.equals("quit")) {
				for (DataServer ds : dataservers) {
					ds.stop();
				}
				break;
			} else {
				String[] s = cmd.split("\\s+");
				if (s.length == 0) {
					continue;
				} else {
					switch (s[0]) {
					case "put":
						if (s.length == 3) {
							put(s[1], s[2], pwd);
						} else {
							System.out.println("put filepath treepath");
						}
						break;
					case "read":
						if (s.length == 3) {
							read(s[1], s[2], pwd);
						} else {
							System.out.println("read treepath filepath or read fileID filepath");
						}
						break;
					case "fetch":
						if (s.length == 4) {
							fetch(s[1], s[2], s[3], pwd);
						} else {
							System.out.println("fetch treepath offset filepath");
						}
						break;
					case "ls":
						ls(pwd);
						break;
					case "pwd":
						System.out.println(mergePath(pwd));
						break;
					case "cd":
						if (s.length == 2) {
							cd(s[1], pwd);
						} else {
							System.out.println("cd dirpath");
						}
						break;
					case "mkdir":
						if (s.length == 2) {
							mkdir(s[1], pwd);
						} else {
							System.out.println("mkdir dirpath");
						}
						break;
					case "locate":
						if (s.length == 2) {
							locate(s[1], pwd);
						} else {
							System.out.println("locate treepath");
						}
						break;
					case "list":
						list();
						break;
					case "recover":
						recover();
						break;
					default:
						System.out.println("put filepath treepath");
						System.out.println("read treepath filepath or read fileID filepath");
						System.out.println("fetch treepath offset filepath or fetch fileID offset filepath");
						System.out.println("cd dirpath");
						System.out.println("locate treepath");
						System.out.println("mkdir treepath");
						System.out.println("ls");
						System.out.println("pwd");
						System.out.println("list");
						System.out.println("recover");
					}
				}
			}
		}
	}

	private void mkdir(String treepath, LinkedList<String> pwd) {
		if (!treepath.substring(0, 1).equals("/")) {
			treepath = mergePath(pwd) + treepath; // 在当前目录往下
		}
		fileTree.addDir(treepath);
	}

	private void recover() {
		LinkedList<byte[]> goodChunks = new LinkedList<>();
		for (int fileid : dsList.keySet()) {
			for (int i = 0; i < chunkNum.get(fileid); ++i) {
				int[] dslist = this.dsList.get(fileid);
				for (int j : dslist) {
					synchronized (dataservers.get(j).command) {
						dataservers.get(j).command.setCmd("read");
						dataservers.get(j).command.setFileID(fileid);
						dataservers.get(j).command.setOffset(i);
					}
					synchronized (dataservers.get(j).cmdFlag) {
						dataservers.get(j).cmdFlag.setFlag(true);
						dataservers.get(j).cmdFlag.notify();
					}
				}
				for (int j : dslist) {
					synchronized (dataservers.get(j).feedbackFlag) {
						try {
							if (!dataservers.get(j).feedbackFlag.getFlag()) {
								dataservers.get(j).feedbackFlag.wait();
							}
							dataservers.get(j).feedbackFlag.setFlag(false);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				byte[] goodChunk = null;
				LinkedList<Integer> badList = new LinkedList<>();
				for (int j : dslist) {
					// 检测md5
					synchronized (dataservers.get(j).command) {
						byte[] chunk = dataservers.get(j).command.getChunk();
						if (getMd5(chunk).equals(this.md5List.get(fileid).get(i))) {
							goodChunk = chunk;
						} else {
							badList.add(j);
						}
					}
				}
				// 恢复
				if (goodChunk == null) {
					System.out.println(fileid + " chunk_" + i + " was lost.");
					return;
				} else {
					goodChunks.add(goodChunk);
					int[] baddslist = new int[badList.size()];
					for (int k = 0; k < badList.size(); ++k) {
						baddslist[k] = badList.get(k);
					}
					putOneChunk(fileid, baddslist, goodChunk, i);
				}
			}
		}
	}

	private void list() {
		System.out.println("File ID\tChunks");
		for (int fileid : dsList.keySet()) {
			System.out.print(fileid+"\t[");
			for (int i=0;i<chunkNum.get(fileid);++i) {
				System.out.print(fileid+"_"+i+", ");
			}
			System.out.println("]\n");
		}
		System.out.println("File ID\tData Servers");
		for (int fileid : dsList.keySet()) {
			System.out.println(fileid+"\t"+Arrays.toString(dsList.get(fileid)));
		}
	}

	private void locate(String treepath, LinkedList<String> pwd) {
		if (!treepath.substring(0, 1).equals("/")) {
			treepath = mergePath(pwd) + treepath; // 在当前目录往下
		}
		if (!fileTree.existFile(treepath)) {
			System.out.println(treepath + " doesn't exist");
			return;
		} else {
			System.out.println(treepath + " exist");
		}
	}

	private void fetch(String treepath, String offset, String filepath, LinkedList<String> pwd) {
		int fileid;
		if (isNumeric(treepath) && Integer.valueOf(treepath) < this.fileIDgenerater) {
			fileid = Integer.valueOf(treepath); // 第二个参数是fileID的情况
		} else {
			if (!treepath.substring(0, 1).equals("/")) {
				treepath = mergePath(pwd) + treepath; // 在当前目录往下
			}
			if (!fileTree.existFile(treepath)) {
				System.out.println(treepath + " doesn't exist");
				return;
			}
			fileid = fileTree.getFileId(treepath);
		}
		
		LinkedList<byte[]> goodChunks = new LinkedList<>();
		if (!isNumeric(offset)) {
			System.out.println("Offset should be an Integer.");
			return;
		} else if (Integer.valueOf(offset)>=chunkNum.get(fileid)) {
			System.out.println("Offset exceed range");
			return;
		}
		
		// 从dataserver中读取chunks
		for (int i = Integer.valueOf(offset); i < chunkNum.get(fileid); ++i) {
			int[] dslist = this.dsList.get(fileid);
			for (int j : dslist) {
				synchronized (dataservers.get(j).command) {
					dataservers.get(j).command.setCmd("read");
					dataservers.get(j).command.setFileID(fileid);
					dataservers.get(j).command.setOffset(i);
				}
				synchronized (dataservers.get(j).cmdFlag) {
					dataservers.get(j).cmdFlag.setFlag(true);
					dataservers.get(j).cmdFlag.notify();
				}
			}
			for (int j : dslist) {
				synchronized (dataservers.get(j).feedbackFlag) {
					try {
						if (!dataservers.get(j).feedbackFlag.getFlag()) {
							dataservers.get(j).feedbackFlag.wait();
						}
						dataservers.get(j).feedbackFlag.setFlag(false);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			byte[] goodChunk = null;
			LinkedList<Integer> badList = new LinkedList<>();
			for (int j : dslist) {
				// 检测md5
				synchronized (dataservers.get(j).command) {
					byte[] chunk = dataservers.get(j).command.getChunk();
					if (getMd5(chunk).equals(this.md5List.get(fileid).get(i))) {
						goodChunk = chunk;
					} else {
						badList.add(j);
					}
				}
			}
			// 恢复
			if (goodChunk == null) {
				System.out.println(fileid + " chunk_" + i + " was lost.");
				return;
			} else {
				goodChunks.add(goodChunk);
				int[] baddslist = new int[badList.size()];
				for (int k = 0; k < badList.size(); ++k) {
					baddslist[k] = badList.get(k);
				}
				putOneChunk(fileid, baddslist, goodChunk, i);
			}
		}
		// 写入文件
		try {
			FileOutputStream out = new FileOutputStream(filepath);
			for (byte[] chunk : goodChunks) {
				out.write(chunk);
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private boolean isNumeric(String str) {
		for (int i = str.length(); --i >= 0;) {
			int chr = str.charAt(i);
			if (chr < 48 || chr > 57)
				return false;
		}
		return true;
	}

	private void read(String treepath, String filepath, LinkedList<String> pwd) {
		int fileid;
		if (isNumeric(treepath) && Integer.valueOf(treepath) < this.fileIDgenerater) {
			fileid = Integer.valueOf(treepath); // 第二个参数是fileID的情况
		} else {
			if (!treepath.substring(0, 1).equals("/")) {
				treepath = mergePath(pwd) + treepath; // 在当前目录往下
			}
			if (!fileTree.existFile(treepath)) {
				System.out.println(treepath + " doesn't exist");
				return;
			}
			fileid = fileTree.getFileId(treepath);
		}
		
		LinkedList<byte[]> goodChunks = new LinkedList<>();
		// 从dataserver中读取chunks
		for (int i = 0; i < chunkNum.get(fileid); ++i) {
			int[] dslist = this.dsList.get(fileid);
			for (int j : dslist) {
				synchronized (dataservers.get(j).command) {
					dataservers.get(j).command.setCmd("read");
					dataservers.get(j).command.setFileID(fileid);
					dataservers.get(j).command.setOffset(i);
				}
				synchronized (dataservers.get(j).cmdFlag) {
					dataservers.get(j).cmdFlag.setFlag(true);
					dataservers.get(j).cmdFlag.notify();
				}
			}
			for (int j : dslist) {
				synchronized (dataservers.get(j).feedbackFlag) {
					try {
						if (!dataservers.get(j).feedbackFlag.getFlag()) {
							dataservers.get(j).feedbackFlag.wait();
						}
						dataservers.get(j).feedbackFlag.setFlag(false);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			byte[] goodChunk = null;
			LinkedList<Integer> badList = new LinkedList<>();
			for (int j : dslist) {
				// 检测md5
				synchronized (dataservers.get(j).command) {
					byte[] chunk = dataservers.get(j).command.getChunk();
					if (getMd5(chunk).equals(this.md5List.get(fileid).get(i))) {
						goodChunk = chunk;
					} else {
						badList.add(j);
					}
				}
			}
			// 恢复
			if (goodChunk == null) {
				System.out.println(fileid + " chunk_" + i + " was lost.");
				return;
			} else {
				goodChunks.add(goodChunk);
				int[] baddslist = new int[badList.size()];
				for (int k = 0; k < badList.size(); ++k) {
					baddslist[k] = badList.get(k);
				}
				putOneChunk(fileid, baddslist, goodChunk, i);
			}
		}
		// 写入文件
		try {
			FileOutputStream out = new FileOutputStream(filepath);
			for (byte[] chunk : goodChunks) {
				out.write(chunk);
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String mergePath(LinkedList<String> pwd) {
		String res = "/";
		for (String p : pwd) {
			res += p;
			res += "/";
		}
		return res;
	}

	private static void cd(String dir, LinkedList<String> pwd) {
		if (dir.equals("..")) {
			pwd.removeLast();
		} else if (dir.equals(".")) {
		} else {
			String[] ss = dir.split("/");
			LinkedList<String> dirs = new LinkedList<>();
			for (String s : ss) {
				if (!s.equals("")) {
					dirs.add(s);
				}
			}
			if (dir.substring(0, 1).equals("/")) {
				LinkedList<String> tmppwd = new LinkedList<>();
				for (String s : dirs) {
					if (fileTree.existDir(tmppwd, s)) {
						tmppwd.add(s);
					} else {
						System.out.println("In " + mergePath(tmppwd) + s + "/ doesn't exist.");
						return;
					}
				}
				pwd.clear();
				pwd.addAll(dirs);
			} else {
				LinkedList<String> tmppwd = new LinkedList<>();
				tmppwd.addAll(pwd);
				for (String s : dirs) {
					if (fileTree.existDir(tmppwd, s)) {
						tmppwd.add(s);
					} else {
						System.out.println("In " + mergePath(tmppwd) + s + "/ doesn't exist.");
						return;
					}
				}
				pwd.clear();
				pwd.addAll(tmppwd);
			}
		}
	}

	private static void ls(LinkedList<String> pwd) {
		// for (int i=0;i<pwd.size();++i) {
		// System.out.print(pwd.get(i));
		// }
		// System.out.println();
		fileTree.display(pwd);
	}

	public void put(String filepath, String treepath, LinkedList<String> pwd) {
		// 判断filepath是否存在
		File file = new File(filepath);
		if (!file.exists()) {
			System.out.println(filepath + " does not exist.");
			return;
		}

		LinkedList<byte[]> chunks = readFile(filepath); // 读取filepath里的文件返回chunks
		int fileID = newFileID(); // 产生fileID
		chunkNum.put(fileID, chunks.size()); // fileID - chunk number
		// 存入文件树
		if (!treepath.substring(0, 1).equals("/"))
			treepath = mergePath(pwd) + treepath; // 在当前目录往下
		int res = fileTree.addFile(treepath, fileID);
		if (res==-1) {
			return;
		}
		// 计算chunks的MD5
		LinkedList<String> chunkMD5 = new LinkedList<>();
		for (byte[] chunk : chunks) {
			chunkMD5.add(getMd5(chunk));
		}
		// 保存MD5
		md5List.put(fileID, chunkMD5);
		// 存入dataserver中
		int[] dslist = bottomList();
		this.dsList.put(fileID, dslist);
		for (int i = 0; i < chunks.size(); ++i) {
			putOneChunk(fileID, dslist, chunks.get(i), i);
		}
		System.out.println("File upload success. File ID: "+fileID);
	}

	public void putOneChunk(int fileID, int[] dslist, byte[] chunk, int offset) {
		for (int j : dslist) {
			synchronized (dataservers.get(j).command) {
				dataservers.get(j).command.setCmd("put");
				dataservers.get(j).command.setFileID(fileID);
				dataservers.get(j).command.setChunk(chunk);
				dataservers.get(j).command.setOffset(offset);
			}
			synchronized (dataservers.get(j).cmdFlag) {
				dataservers.get(j).cmdFlag.setFlag(true);
				dataservers.get(j).cmdFlag.notify();
			}
		}
		for (int j : dslist) {
			synchronized (dataservers.get(j).feedbackFlag) {
				try {
					if (!dataservers.get(j).feedbackFlag.getFlag()) {
						dataservers.get(j).feedbackFlag.wait();
					}
					dataservers.get(j).feedbackFlag.setFlag(false);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private int[] bottomList() {
		int[] list = new int[repNum];
		for (int i = 0; i < repNum; ++i) {
			list[i] = i;
		}
		for (int i = repNum; i < dataservers.size(); ++i) {
			double maxsize = -1;
			int maxindex = 0;
			for (int j = 0; j < repNum; ++j) {
				if (maxsize < dataservers.get(j).size) {
					maxsize = dataservers.get(j).size;
					maxindex = j;
				}
			}
			if (maxsize > dataservers.get(i).size) {
				list[maxindex] = i;
			}
		}
		return list;
	}

	// 读取文件filePath，返回byte[]列表
	public LinkedList<byte[]> readFile(String filePath) {
		File file = new File(filePath);
		InputStream in = null;
		LinkedList<byte[]> chunks = new LinkedList<byte[]>();
		try {
			in = new FileInputStream(file);
			int byteread = 0;
			byte[] chunk = new byte[chunkSize];
			while ((byteread = in.read(chunk)) != -1) {
				chunks.add(Arrays.copyOfRange(chunk, 0, byteread));
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return chunks;
	}

	// 递增fileID
	public static int newFileID() {
		return fileIDgenerater++;
	}

	// 返回一个chunk的md5
	public static String getMd5(byte[] chunk) {
		return DigestUtils.md5Hex(chunk);
	}

	// 添加data server
	public void addDS(DataServer ds) {
		dataservers.add(ds);
	}
}

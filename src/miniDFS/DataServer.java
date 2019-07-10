package miniDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;

public class DataServer extends Thread {

	public final int chunkSize = 2 * 1024 * 1024;
	private String dsName;
	final public Flag cmdFlag = new Flag(false);
	final public Flag feedbackFlag = new Flag(false);
	final public Command command = new Command();
	public double size = 0;

	DataServer(String name) {
		dsName = name;
		File file = new File(name);
		if (file.exists() && file.isDirectory()) {
//			System.out.println(name + " exists");
		} else {
			file.mkdir();
		}
	}

	@Override
	public void run() {
		while (true) {
			// 等待命令并执行
			synchronized (cmdFlag) {
				try {
					if (!cmdFlag.getFlag()) {
						cmdFlag.wait();
					}
					cmdFlag.setFlag(false);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
			// 执行命令
			if (command.getCmd().equals("put")) {
//				System.out.println("File ID: "+command.getFileID()+"\tOffset: "+command.getOffset()+"\t");
				put(command.getFileID(), command.getOffset(), command.getChunk());
				size += command.getChunk().length;
			} else if (command.getCmd().equals("read")) {
				byte[] chunk = fetch(command.getFileID(), command.getOffset());
				command.setChunk(chunk);
			}
			
			// 通知NameServer
			synchronized (feedbackFlag) {
				feedbackFlag.setFlag(true);
				feedbackFlag.notify();
			}
		}
	}

	// 写入一个chunk
	public void put(int fileID, int offset, byte[] chunk) {
		OutputStream out = null;
		String chunkName = dsName + "/" + fileID + "_" + offset;
		try {
			out = new FileOutputStream(chunkName);
			out.write(chunk);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 读取一个chunk
	public byte[] fetch(int fileID, int offset) {
		File file = new File(dsName + "/" + fileID + "_" + offset);
		if (file.exists()) {
			try {
				InputStream in = new FileInputStream(file);
				byte[] chunk = new byte[in.available()];
				in.read(chunk);
				in.close();
				return chunk;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		} else {
			System.out.println(dsName + "/" + fileID + "_" + offset + " doesn't exist!");
			return new byte[1];
		}
	}

	// 读取文件fileID的所有chunk，返回byte[]列表
//	public LinkedList<byte[]> read(String fileID, String filePath) {
//		int offset = 0;
//		InputStream in = null;
//		LinkedList<byte[]> chunks = new LinkedList<byte[]>();
//		try {
//			while (true) {
//				File file = new File(dsName + "/" + fileID + "_" + offset);
//				if (file.exists()) {
//					in = new FileInputStream(file);
//					byte[] bytes = new byte[(int) file.length()];
//					in.read(bytes);
//					chunks.add(bytes);
//					in.close();
//				} else {
//					break;
//				}
//				offset++;
//			}
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//		return chunks;
//	}
}

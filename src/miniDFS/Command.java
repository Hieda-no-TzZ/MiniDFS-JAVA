package miniDFS;

public class Command {
	private String cmd;
	private String filepath;
	private String treepath;
	private int offset;
	private byte[] chunk;
	private int fileID;
	
	public String getCmd() {
		return cmd;
	}
	public void setCmd(String cmd) {
		this.cmd = cmd;
	}
	public String getFilepath() {
		return filepath;
	}
	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}
	public String getTreepath() {
		return treepath;
	}
	public void setTreepath(String treepath) {
		this.treepath = treepath;
	}
	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
	public byte[] getChunk() {
		return chunk;
	}
	public void setChunk(byte[] chunk) {
		this.chunk = chunk;
	}
	public int getFileID() {
		return fileID;
	}
	public void setFileID(int fileID) {
		this.fileID = fileID;
	}
}

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Client {
	public static void main(String[] args) {	
		System.out.println("Client Started !!");
		
		File[] chunks = null;
		List<String> chunksReceived = new ArrayList<String>();
		List<String> chunksRemaining = new ArrayList<String>();
		
		int serverPort = ClientUtility.getServerPort();
		ClientHandler serverDownloadHandler = new ClientHandler(TYPE.ServerDownloadHandler, serverPort, chunksReceived, chunksRemaining);
		serverDownloadHandler.start();
		
		while(serverDownloadHandler.isAlive());
		int clientId = serverDownloadHandler.getClientId();
		String fileName = serverDownloadHandler.getFileName();
		chunks = serverDownloadHandler.getChunks();
		
		String[] peerConfig = ClientUtility.getPeerConfig(clientId);
		int downloadNeighborPort = Integer.parseInt(peerConfig[2]);
		int uploadNeighborPort = Integer.parseInt(peerConfig[1]);
		
		if(0 < chunksRemaining.size()) {
			ClientHandler neighborDownloadHandler = new ClientHandler(TYPE.NeighborDownloadHandler, downloadNeighborPort, chunksReceived, chunksRemaining);
			neighborDownloadHandler.setClientId(clientId);
			neighborDownloadHandler.setFileName(fileName);
			neighborDownloadHandler.setChunks(chunks);
			neighborDownloadHandler.start();
		} else {
			ClientUtility.mergeFiles(Arrays.asList(chunks));
		}
		
		ClientHandler neighborUploadHandler = new ClientHandler(TYPE.NeighborUploadHandler, uploadNeighborPort, chunksReceived, chunksRemaining);
		neighborUploadHandler.setClientId(clientId);
		neighborUploadHandler.setFileName(fileName);
		neighborUploadHandler.setChunks(chunks);
		neighborUploadHandler.start();
	}
}

enum TYPE { ServerDownloadHandler, NeighborDownloadHandler, NeighborUploadHandler };

class ClientHandler extends Thread {
	private static final String ROOT_DIR = "data\\";
	
	private TYPE type;
	private int clientId;
	private int port;
	private ServerSocket listener;
	private Socket connection; 
	private ObjectOutputStream os;
	private ObjectInputStream is;
	private String fileName;
	private List<String> chunksReceived;
	private List<String> chunksRemaining;
	private File[] chunks;
	
	public ClientHandler(TYPE type, int port, List<String> chunksReceived, List<String> chunksRemaining) {
		this.type = type;
		this.port = port;
		this.chunksReceived = chunksReceived;
		this.chunksRemaining = chunksRemaining; 
	}
	
	public void setClientId(int clientId) {
		this.clientId = clientId;
	}
	
	public int getClientId() {
		return this.clientId;
	}
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public File[] getChunks() {
		return chunks;
	}

	public void setChunks(File[] chunks) {
		this.chunks = chunks;
	}

	public void run() {
		switch(type) {
			case ServerDownloadHandler: handleDownloadFromServer();
			break;
			case NeighborDownloadHandler: handleDownloadFromNeighbor();
			break;
			case NeighborUploadHandler: handleUploadToNeighbor();
			break;
		}
	}
	
	private void handleDownloadFromServer() {
		try {
			connection = new Socket("localhost", port);
			System.out.println("Connected to Server on port " + port);
			
			initializeInputOutputStreams();

			boolean flag = true;
			while (flag) {
				String msg = is.readUTF();
				String[] str = msg.split(":");
				switch(str[0]) {
					case "CLIENT_ID": this.clientId = Integer.parseInt(str[1]);
						System.out.println("Client Id: " + this.clientId);
					break;
					case "FILE_NAME": this.fileName = str[1];
					System.out.println("File name: " + this.fileName);
					break;
					case "FILE_SIZE": long fileSize = Long.parseLong(str[1]);
					System.out.println("File size: " + fileSize);
					break;
					case "NUM_CHUNKS": int numChunks = Integer.parseInt(str[1]);
					System.out.println("Number of chunks: " + numChunks);
					
					chunks = new File[numChunks];
					for(int i=1; i<=numChunks; i++) {
						chunksRemaining.add("" + i);
					}
					break;
					case "CHUNK": String chunkNum = str[1];
						int chunkSize = Integer.parseInt(str[2]);
						receiveChunk(chunkNum, chunkSize);
						System.out.println("DOWNLOAD: Received chunk " + chunkNum + " of size " + chunkSize + " bytes from Server");
					break;
					case "DONE": flag = false;
					break;
				}
			}
		} catch (ConnectException e) {
			System.err.println("Connection refused !! You need to initiate a server first !!");
		} catch (UnknownHostException unknownHost) {
			System.err.println("You are trying to connect to an unknown host !!");
		} catch (IOException ioException) {
			ioException.printStackTrace();
		} finally {
			// Close connections
			try {
				if(null != is) {
					is.close();
				}
				if(null != os) {
					os.close();
				}
				if(null != connection) {
					connection.close();
				}
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}
	}
	
	private void handleDownloadFromNeighbor() {
		try {
			boolean connected = false;
			while(!connected) {
				try {
					connection = new Socket("localhost", port);
					System.out.println("Connected to download neighbor on port " + port);
					connected = true;
				} catch(ConnectException ce) {
					try {
						System.out.println("Download neighbor offline. Retry after 1 sec");
						Thread.sleep(1000);
					} catch(InterruptedException ie) {
						ie.printStackTrace();
					}
				}
			}
			
			initializeInputOutputStreams();

			String req = null, res = null;
			
			while (0 < chunksRemaining.size()) {
				System.out.println("DOWNLOAD: Requesting chunk list from download neighbor");
				req = "REQ_FOR_CHUNK_LIST";
				sendMessage(req);
				
				res = is.readUTF(); //comma separated list of chunk ids
				System.out.println("DOWNLOAD: Received chunk list from download neighbor - [" + res + "]");
				
				String[] str = res.split(",");
				if(!str[0].equals("NONE")) {
					List<String> downloadNeighborChunkIds = new ArrayList<String>(Arrays.asList(str));
					
					downloadNeighborChunkIds.removeAll(chunksReceived);
					if(downloadNeighborChunkIds.size() > 0) {
						for(String chunkId: downloadNeighborChunkIds) {
							System.out.println("DOWNLOAD: Requesting chunk " + chunkId + " from download neighbor");
							req = "REQ_FOR_CHUNK:" + chunkId;
							sendMessage(req);
							
							int chunkSize = is.readInt();
							receiveChunk(chunkId, chunkSize);
							System.out.println("DOWNLOAD: Received chunk " + chunkId + " of size " + chunkSize + " bytes from download neighbor");
						}
					} else {
						System.out.println("DOWNLOAD: No new chunks available at download neigbor");
					}
				}
				
				try {
					Thread.sleep(1000);
				} catch(InterruptedException ie) {
					ie.printStackTrace();
				}
			}
			
			req = "DONE";
			sendMessage(req);
			System.out.println("DOWNLOAD: File download complete");
			
			ClientUtility.mergeFiles(Arrays.asList(chunks));
			
		} catch (ConnectException e) {
			System.err.println("Connection refused !!");
		} catch (UnknownHostException unknownHost) {
			System.err.println("You are trying to connect to an unknown host !!");
		} catch (IOException ioException) {
			ioException.printStackTrace();
		} finally {
			// Close connections
			try {
				if(null != is) {
					is.close();
				}
				if(null != os) {
					os.close();
				}
				if(null != connection) {
					connection.close();
				}
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}
	}
	
	private void handleUploadToNeighbor() {
		try {
			listener = new ServerSocket(port);
			connection = listener.accept();
			System.out.println("Upload neighbor is connected");
			
			initializeInputOutputStreams();
			
			String req = null, res = null;
			boolean isUploadComplete = false;
			while(!isUploadComplete) {
				req = is.readUTF();
				String[] str = req.split(":");
				
				switch(str[0]) {
					case "REQ_FOR_CHUNK_LIST": 
						System.out.println("UPLOAD: Received request for chunk list from upload neighbor");
						String chunkList = chunksReceived.size() > 0 ? getReceivedChunksList() : "NONE"; 
						sendMessage(chunkList);
						System.out.println("UPLOAD: Chunk list sent to upload neighbor - [" + chunkList + "]");
					break;
					
					case "REQ_FOR_CHUNK": 
						int chunkNum = Integer.parseInt(str[1]);
						System.out.println("UPLOAD: Received request for chunk " + chunkNum + " from upload neighbor");
						File chunk = chunks[chunkNum-1];
						
						int chunkSize = (int) chunk.length();
						os.writeInt(chunkSize);
						os.flush();
						
						System.out.println("UPLOAD: Sending chunk " + chunkNum + " of size " + chunkSize + " bytes to upload neighbor");
						sendChunk(chunk, chunkSize);
					break;
					
					case "DONE": isUploadComplete = true;
						System.out.println("UPLOAD: File upload complete");
					break;
				}
			}
			
			
		} catch(IOException ie) {
			ie.printStackTrace();
		}
	}
	
	private String getReceivedChunksList() {
		String res = "";
		for(String chunkId: chunksReceived) {
			res += chunkId + ",";
		}
		
		return res.substring(0, res.length()-1);
	}
	
	private void receiveChunk(String chunkId, int chunkSize) {
		File chunkDir = null;
		File chunk = null;
		FileOutputStream fos = null;
		BufferedOutputStream bos = null;
		byte[] buff = null;
		
		try {
			chunkDir = new File(ROOT_DIR, "Client" + clientId + "\\part");
			if(!chunkDir.exists()) {
				chunkDir.mkdirs();
			}
			
			if(null == buff) {
				buff = new byte[chunkSize];
			}
			
			is.readFully(buff);
			
			int chunkNum = Integer.parseInt(chunkId);
			chunk = new File(chunkDir.getPath(), this.fileName + "." + String.format("%03d", chunkNum));
			fos = new FileOutputStream(chunk);
			bos = new BufferedOutputStream(fos);
			bos.write(buff, 0, chunkSize);
			
			bos.close();
			fos.close();
			
			chunks[chunkNum-1] = chunk;
			chunksRemaining.remove(chunkId);
			chunksReceived.add(chunkId);
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(null != bos) {
					bos.close();
				}
				if(null != fos) {
					fos.close();
				}
			} catch(IOException ie) {
				ie.printStackTrace();
			}
		}
	}
	
	public void sendChunk(File chunk, int chunkSize) {
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		byte[] buff = null;
		
		try {
			buff = new byte[chunkSize];
			fis = new FileInputStream(chunk);
			bis = new BufferedInputStream(fis);
			
			bis.read(buff, 0, chunkSize);
			os.write(buff, 0, chunkSize);
			
			os.flush();
		} catch (IOException ioException) {
			ioException.printStackTrace();
		} finally {
			try {
				if(null != bis) {
					bis.close();
				}
				if(null != fis) {
					fis.close();
				}
			} catch(IOException ie) {
				ie.printStackTrace();
			}
		}
	}
	
	private void sendMessage(String msg) {
		try {
			os.writeUTF(msg);
			os.flush();
		} catch (IOException ie) {
			ie.printStackTrace();
		}
	}
	
	// initialize inputStream and outputStream
	private void initializeInputOutputStreams() throws IOException {
		os = new ObjectOutputStream(connection.getOutputStream());
		os.flush();
		is = new ObjectInputStream(connection.getInputStream());
	}
}

class ClientUtility {
	private static final String SERVER_CONFIG_FILE = "server.cfg";
	private static final String PEERS_CONFIG_FILE = "peers.cfg";
	
	public static int getServerPort() {
		BufferedReader br = null;
		int port = 0;
		try {
			br = new BufferedReader(new FileReader(SERVER_CONFIG_FILE));
			port = Integer.parseInt(br.readLine().split("=")[1]);
		} catch(IOException ie) {
			ie.printStackTrace();
		} finally {
			if(null != br) {
				try{
					br.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return port;
	}
	
	public static String[] getPeerConfig(int clientId) {
		BufferedReader br = null;
		String[] config = null;
		try {
			br = new BufferedReader(new FileReader(PEERS_CONFIG_FILE));
			String line = br.readLine(); //reading header
			while(null != (line = br.readLine())) {
				config = line.split(",");
				int peerId = Integer.parseInt(config[0]);
				if(clientId == peerId) {
					break;
				} else {
					config = null;
				}
			}
		} catch(IOException ie) {
			ie.printStackTrace();
		} finally {
			if(null != br) {
				try{
					br.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return config;
	}
	
	public static void mergeFiles(List<File> files) {
		System.out.println("Merging chunks into one file");
		FileOutputStream mergingStream = null;
		try {
			File filePart0 = files.get(0);
			String filePart0Name = filePart0.getName();
			
			File destFileDir = new File(filePart0.getParentFile().getParent(), "completed");
			if(!destFileDir.exists()) {
				destFileDir.mkdir();
			}
			
			String destFileName = filePart0Name.substring(0, filePart0.getName().lastIndexOf('.'));
			File destFile = new File(destFileDir, destFileName);
			
			mergingStream = new FileOutputStream(destFile);
			for (File f : files) {
				Files.copy(f.toPath(), mergingStream);
			}
			
			System.out.println("Merged file location: " + destFileDir.getPath());
			System.out.println("Merged file name: " + destFileName);
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(null != mergingStream) {
				try{
					mergingStream.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static List<File> listOfFilesToMerge(File oneOfFiles) {
		String tmpName = oneOfFiles.getName(); // {name}.{number}
		String destFileName = tmpName.substring(0, tmpName.lastIndexOf('.')); // remove .{number}
		
		File[] files = oneOfFiles.getParentFile().listFiles(
				(File dir, String name) -> name.matches(destFileName + "[.]\\d+"));
		
		Arrays.sort(files); // ensuring order 001, 002, ..., 010, ...
		return Arrays.asList(files);
	}

	public static void mergeFiles(File oneOfFiles) {
		mergeFiles(listOfFilesToMerge(oneOfFiles));
	}

	private static List<File> listOfFilesToMerge(String oneOfFiles) {
		return listOfFilesToMerge(new File(oneOfFiles));
	}

	public static void mergeFiles(String oneOfFiles) {
		mergeFiles(new File(oneOfFiles));
	}
}
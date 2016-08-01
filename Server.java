import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class Server {

	public static void main(String[] args) throws IOException {
		System.out.println("Server Started !!");
		
		ServerUtility.loadProperties();
		String fileLoc = ServerUtility.getFileLoc();
		int chunkSize = ServerUtility.getChunkSize();
		int sPort = ServerUtility.getPort();
		int numClients = ServerUtility.getNumClients();
		
		Scanner sc = new Scanner(System.in);
		System.out.println("Make sure the file to be uploaded exits at location " + fileLoc);
		System.out.println("Enter file name: ");
		String fileName = sc.nextLine();
		sc.close();
		
		File file = new File(fileLoc, fileName);
		long fileSize = file.length();
		
		List<File> partFiles = ServerUtility.splitFileIntoChunks(file, chunkSize);
			
		System.out.println("File name: " + fileName);
		System.out.println("File size: " + fileSize);
		System.out.println("Number of chunks: " + partFiles.size());
		
		ServerSocket listener = new ServerSocket(sPort);
		int clientNum = 1;
		try {
			while (clientNum <= 5) {
				new ServerHandler(listener.accept(), clientNum, file.getName(), fileSize, numClients, partFiles).start();
				System.out.println("Client " + clientNum + " connected to Server !!");
				clientNum++;
			}
		} finally {
			listener.close();
		}
	}
}

/**
 * A handler thread class. Handlers are spawned from the listening loop and
 * are responsible for dealing with a single client's requests.
 */
class ServerHandler extends Thread {
	private Socket connection;
	private ObjectInputStream is; 
	private ObjectOutputStream os; 
	private int clientId;
	private String filename;
	private long fileSize;
	private int numClients;
	private List<File> partFiles;

	public ServerHandler(Socket connection, int clientId, String filename, long fileSize, int numClients, List<File> partFiles) {
		this.connection = connection;
		this.clientId = clientId;
		this.filename = filename;
		this.fileSize = fileSize;
		this.numClients = numClients;
		this.partFiles = partFiles;
	}

	public void run() {
		int numChunks = partFiles.size();
		try {
			// initialize Input and Output streams
			os = new ObjectOutputStream(connection.getOutputStream());
			os.flush();
			is = new ObjectInputStream(connection.getInputStream());
			
			sendMessage("CLIENT_ID:" + clientId);
			sendMessage("FILE_NAME:" + filename);
			sendMessage("FILE_SIZE:" + fileSize);
			sendMessage("NUM_CHUNKS:" + numChunks);
			
			File chunk = null;
			int chunkNum = clientId;
			while(chunkNum <= numChunks) {
				chunk = partFiles.get(chunkNum-1);
				
				int chunkSize = (int) chunk.length();
				sendMessage("CHUNK:" + chunkNum + ":" + chunkSize);
				
				System.out.println("Sending chunk " + chunkNum + " of size " + chunkSize + " bytes to Client " + clientId);
				sendChunk(chunk, chunkSize);
				
				chunkNum += numClients;
			}
			sendMessage("DONE");
			System.out.println("Finished sending to Client " + clientId);
		} catch (IOException ioException) {
			System.out.println("Disconnect with Client " + clientId);
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
				System.out.println("Disconnect with Client " + clientId);
			}
		}
	}

	// send a message to the output stream
	public void sendMessage(String msg) {
		try {
			os.writeUTF(msg);
			os.flush();
		} catch (IOException ioException) {
			ioException.printStackTrace();
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
}

class ServerUtility {
	private static final String CONFIG_FILE = "server.cfg";
	public static Properties prop;
	
	public static void loadProperties() {
		prop = new Properties();
		try {
			prop.load(new FileInputStream(CONFIG_FILE));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static int getPort() {
		return Integer.parseInt(prop.getProperty("PORT"));
	}
	
	public static int getNumClients() {
		return Integer.parseInt(prop.getProperty("NUM_CLIENTS"));
	}
	
	public static int getChunkSize() {
		return Integer.parseInt(prop.getProperty("CHUNK_SIZE"));
	}
	
	public static String getFileLoc() {
		return prop.getProperty("FILE_LOC");
	}
	
	/**
	 * @param file: file name & location
	 * @param chunkSize: chunk size in bytes
	 */
	public static List<File> splitFileIntoChunks(File file, int chunkSize) {
		int partCounter = 1;
		byte[] buffer = new byte[chunkSize];
		List<File> partFiles = new ArrayList<File>();

		BufferedInputStream bis = null;
		FileOutputStream out = null;
		try {
			bis = new BufferedInputStream(new FileInputStream(file));
			String name = file.getName();

			int tmp = 0;
			while ((tmp = bis.read(buffer)) > 0) {
				// write each chunk of data into separate file with different number in name
				File newFile = new File(file.getParent(), name + "." + String.format("%03d", partCounter++));
				out = new FileOutputStream(newFile);
				out.write(buffer, 0, tmp);// tmp is chunk size
				partFiles.add(newFile);
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(null != bis) {
					bis.close();
				}
				if(null != out) {
					out.close();
				}
			} catch(IOException ie) {
				ie.printStackTrace();
			}
		}
		
		return partFiles;
	}
	
	public static void removeSplitFiles(File file, int numChunks) {
		for(int i=1; i<=numChunks; i++) {
			File partFile = new File(file.getParent(), file.getName() + "." + String.format("%03d", i));
			partFile.delete();
		}
	}
}

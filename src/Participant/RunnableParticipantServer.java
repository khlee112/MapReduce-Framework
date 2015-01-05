package Participant;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import Messages.Message;
import Messages.MessageConnectionInit;
import Messages.MessageType;

/**
 * Handle accepting loop to accept connection from master and mapper
 * */
public class RunnableParticipantServer implements Runnable {

	private Participant participant;
	private int port;
	private volatile boolean shouldStop;
	private ServerSocket serverSocket;

	public RunnableParticipantServer(Participant participant, int port) {
		this.participant = participant;
		this.port = port;
	}

	public void stop() {
		shouldStop = true;
		try {
			if (!serverSocket.isClosed()) serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		this.shouldStop = false;
		try {
			serverSocket = new ServerSocket(port);
			while (true && !shouldStop) {
				Socket s = serverSocket.accept();

				// Get I/O stream
				ObjectOutputStream ObjectOS = new ObjectOutputStream(
						s.getOutputStream());
				ObjectOS.flush();
				ObjectInputStream ObjectIS = new ObjectInputStream(
						s.getInputStream());

				// Connection Initialization
				Message message;
				try {
					message = (Message) ObjectIS.readObject();
					if (message.type == MessageType.CONNECTION_INIT) {
						MessageConnectionInit messageInit = (MessageConnectionInit) message;
						if (messageInit.senderID == -1) {
							// participant getting Connection from master
							participant.onMasterConnectionRecieved(s, ObjectOS,
									ObjectIS, message.jobID);
						} else {
							// Participant getting Connection from participant,
							// connection will not be cached
							participant.onParticipantConnectionRecieved(s,
									ObjectOS, ObjectIS);
						}
					} else {
						System.out.println("Participant@" + port
								+ " getting improper init message type"
								+ message.type);
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			shouldStop = true;
			// Do nothing, service is not closed
		}
	}

}

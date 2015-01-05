package Master;

import java.io.EOFException;
import java.io.ObjectInputStream;
import java.net.Socket;

import Messages.Message;

/**
 * 
 * This class handle reading loop for sockets on master Instances of this class
 * are used in PartcipantStub class
 * 
 */

public class RunnableMasterRead implements Runnable {

	private Master master;
	private Socket s;
	private boolean shouldStop;
	private ObjectInputStream mObjectIS;

	public RunnableMasterRead(Master master, Socket s,
			ObjectInputStream mObjectIS) {
		this.master = master;
		this.s = s;
		this.mObjectIS = mObjectIS;
		shouldStop = false;
	}

	public void stop() {
		shouldStop = true;
	}

	@Override
	public void run() {
		while (!shouldStop) {
			try {
				Message message = (Message) mObjectIS.readObject();
				master.receiveMessage(message);
			} catch (EOFException e) {
				shouldStop = true;
			} catch (Exception e) {

				// Kill by master
				// e.printStackTrace();
				// System.err.println("Error: master getting I/O streams from client.");
				try {
					s.close();
				} catch (Exception e1) {
					// e1.printStackTrace();
				}
				return;
			}
		}
		return;
	}
}

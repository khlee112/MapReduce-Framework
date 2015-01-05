package Participant;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import Messages.Message;
import Messages.MessageConnectionInit;
import Messages.MessageType;

/** Abstract class representing a mapper or a reducer. */
abstract public class Participant {

	protected Hashtable<Integer, ParticipantComm> participantComms;
	protected RunnableParticipantServer server;
	protected ParticipantInfo participantInfo;

	public Participant(ParticipantInfo participantInfo) {
		this.participantInfo = participantInfo;
		participantComms = new Hashtable<Integer, ParticipantComm>();
		server = new RunnableParticipantServer(this, participantInfo.port);
		Thread serverThread = new Thread(server);
		serverThread.start();
	}

	public void onMasterConnectionRecieved(Socket toMaster,
			ObjectOutputStream mObjectOS, ObjectInputStream mObjectIS, int jobID) {
		ParticipantComm newComm = new ParticipantComm(jobID, this, toMaster,
				mObjectOS, mObjectIS);
		participantComms.put(jobID, newComm);
	}

	public void onParticipantConnectionRecieved(Socket toParticipant,
			ObjectOutputStream objectOS, ObjectInputStream objectIS) {

		try {
			Message message = (Message) objectIS.readObject();
			toParticipant.close();
			receiveMessage(message);
		} catch (Exception e) {
			e.printStackTrace();
			// System.out.println("Exception: " + e.toString());
		}
	}

	/** Override to specify behavior for handling messages */
	abstract public void receiveMessage(Message message);

	protected void sendConfirmAlive(int jobID) {
		// Master checking if alive, return confirmation
		Message returnMessage = new Message(MessageType.CONFIRM_ALIVE, getID(),
				jobID);
		sendMessageToMaster(returnMessage, jobID);
	}

	protected void sendMessageToMaster(Message message, int jobID) {
		// Find the master corresponding to given jobID and send to it

		if (participantComms.get(jobID) != null) {
			participantComms.get(jobID).sendMessageToMaster(message);
		}
	}

	protected void sendMessageToParticipant(Message message,
			ParticipantInfo participantInfo, int jobID) {
		try {
			// We do not cache connection from mapper to reducer
			Socket toReducer = new Socket(participantInfo.host,
					participantInfo.port);
			ObjectOutputStream reducerObjectOS = new ObjectOutputStream(
					toReducer.getOutputStream());
			reducerObjectOS.flush();
			MessageConnectionInit initMessage = new MessageConnectionInit(
					getID(), jobID);
			reducerObjectOS.writeObject(initMessage);
			reducerObjectOS.writeObject(message);

			// reducerObjectOS.close();
			// toReducer.close();
		} catch (IOException e) {
			// Killed by master
			// System.out.println("Error: Send message to reducer");
			// e.printStackTrace();
			// sendMessageToParticipant(message,participantInfo, jobID);
		}
	}

	public int getID() {
		return participantInfo.id;
	}

	// Stop any communication threads
	protected void stop() {
		
		System.out.println("Participant " + getID() + " stopping");
		System.exit(1);
		
		// stop server thread
		server.stop();
		// stop reading threads
		Iterator<Map.Entry<Integer, ParticipantComm>> it = participantComms
				.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, ParticipantComm> pairs = (Map.Entry<Integer, ParticipantComm>) it
					.next();
			pairs.getValue().stop();
			System.out.println("Participant " + getID() + " stopping");
			it.remove(); // avoids a ConcurrentModificationException
		}
	}

}

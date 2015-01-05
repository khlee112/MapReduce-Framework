package Participant;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import Messages.Message;

public class ParticipantComm {
	protected int jobID;
	protected Participant participant;
	protected Socket toMaster;
	protected ObjectOutputStream mObjectOS;
	protected ObjectInputStream mObjectIS;
	protected RunnableParticipantRead comm;

	public ParticipantComm(int jobID, Participant participant, Socket toMaster,
			ObjectOutputStream mObjectOS, ObjectInputStream mObjectIS) {
		this.jobID = jobID;
		this.participant = participant;
		this.toMaster = toMaster;
		this.mObjectIS = mObjectIS;
		this.mObjectOS = mObjectOS;

		comm = new RunnableParticipantRead(toMaster, participant,
				mObjectIS);
		Thread commThread = new Thread(comm);
		commThread.start();
	}
	
	public void stop() {
		comm.stop();
		try {
			if (!toMaster.isClosed()) toMaster.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void sendMessageToMaster(Message message) {
		try {
			if (toMaster.isConnected()) mObjectOS.writeObject(message);
		} catch (IOException e) {
			System.out.println("Error: participant sendMessageToMaster");
			//e.printStackTrace();
		}
	}

}

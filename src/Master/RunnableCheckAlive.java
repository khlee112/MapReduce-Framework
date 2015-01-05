package Master;

import java.util.List;

import Messages.Message;
import Messages.MessageType;
import Participant.ParticipantInfo;

/** Runnable used by master to send check alive messages to participants. */
public class RunnableCheckAlive implements Runnable {

	private static final int POLL_PERIOD = 1000;

	private Master master;
	private List<ParticipantStub> participants;
	private boolean shouldStop = false;

	// Records whether confirm alive has been received after last round
	private boolean[] confirmAliveReceived;

	public RunnableCheckAlive(Master master) {
		this.master = master;
		this.participants = master.getAllParticipants();
		this.confirmAliveReceived = new boolean[participants.size()];
	}

	@Override
	public void run() {
		while (!shouldStop) {

			confirmAliveReceived = new boolean[participants.size()];

			// Send message to each participant
			for (int i = 0; i < participants.size(); i++) {
				Message checkAlive = new Message(MessageType.CHECK_ALIVE, -1,
						master.job.jobID);
				ParticipantInfo info = participants.get(i).getParticipantInfo();
				master.sendMessageToParticipant(info, checkAlive);
			}

			// Delay before checking status
			try {
				Thread.sleep(POLL_PERIOD);
			} catch (InterruptedException e) {
				System.out.println("Error: " + e.toString());
			}

			// Check if everyone is alive
			for (int i = 0; i < participants.size(); i++) {
				// System.out.println("check");
				if (!confirmAliveReceived[i]) {
					int id = participants.get(i).getID();
					master.onParticipantDied(id);
				}
			}
		}
//		System.out.println("Check alive thread stopped.");
	}

	public void stop() {
		shouldStop = true;
	}

	public void stopSilent() {
		shouldStop = true;
	}

	public synchronized void onConfirmaAliveReceived(int participantID) {
		for (int i = 0; i < participants.size(); i++) {
			if (participants.get(i).getID() == participantID) {
				confirmAliveReceived[i] = true;
				break;
			}
		}
	}
}

package rs.etf.sab.student;

import java.util.Calendar;

public abstract class ScheduledEvent implements Comparable<ScheduledEvent> {

	private Calendar timestamp;

	public ScheduledEvent(Calendar timestamp) {
		this.timestamp = timestamp;
	}

	public abstract void execute();

	@Override
	public int compareTo(ScheduledEvent o) {
		return timestamp.compareTo(o.timestamp);
	}

	public Calendar getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Calendar timestamp) {
		this.timestamp = timestamp;
	}

}

package rs.etf.sab.student;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.PriorityQueue;

import rs.etf.sab.operations.GeneralOperations;

public class vl190384_GeneralOperations implements GeneralOperations {

	private static final String eraseAllQuerry[] = { "DELETE FROM ", " WHERE 1=1;" };
	private static final String eraseAllTables[] = { "[dbo].[Transaction]", "[dbo].[OrderHasArticle]", "[dbo].[Order]",
			"[dbo].[Article]", "[dbo].[Shop]", "[dbo].[Buyer]", "[dbo].[Connection]", "[dbo].[City]" };

	private Calendar currentTime = Calendar.getInstance();

	private PriorityQueue<ScheduledEvent> queue = new PriorityQueue<>();

	@Override
	public void eraseAll() {
		try (Statement s = DB.getInstance().getConnection().createStatement()) {
			for (String table : eraseAllTables)
				s.addBatch(eraseAllQuerry[0] + table + eraseAllQuerry[1]);
			s.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Calendar getCurrentTime() {
		return (Calendar) currentTime.clone();
	}

	@Override
	public void setInitialTime(Calendar time) {
		currentTime.setTimeInMillis(time.getTimeInMillis());
		;
	}

	@Override
	public Calendar time(int days) {
		currentTime.add(Calendar.DAY_OF_MONTH, days);
		ScheduledEvent ev = queue.peek();
		while (ev != null && ev.getTimestamp().compareTo(currentTime) <= 0) {
			queue.poll().execute();
			ev = queue.peek();
		}
		return (Calendar) currentTime.clone();
	}

	public void registerEvent(ScheduledEvent ev) {
		queue.add(ev);
	}

}

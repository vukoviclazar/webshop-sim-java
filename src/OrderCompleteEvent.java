package rs.etf.sab.student;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

public class OrderCompleteEvent extends ScheduledEvent {

	private static final String orderCompleteQuery = "UPDATE [dbo].[Order] "
			+ "SET [Status] = 'arrived', [ReceivedTime] = ? WHERE [IdOrder] = ?";
	private int orderId;
	private Date date;

	public OrderCompleteEvent(Calendar timestamp, int orderId, Date date) {
		super(timestamp);
		this.orderId = orderId;
		this.date = date;
	}

	@Override
	public void execute() {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(orderCompleteQuery)) {
			ps.setDate(1, date);
			ps.setInt(2, orderId);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

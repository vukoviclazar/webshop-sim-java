package rs.etf.sab.student;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

public class LocationUpdateEvent extends ScheduledEvent {

	private static final String locationUpdateQuery = "UPDATE [dbo].[Order] "
			+ "SET [Location] = ? WHERE [IdOrder] = ?";
	private int orderId;
	private int locationId;

	public LocationUpdateEvent(Calendar timestamp, int orderId, int locationId) {
		super(timestamp);
		this.orderId = orderId;
		this.locationId = locationId;
	}

	@Override
	public void execute() {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(locationUpdateQuery)) {
			ps.setInt(1, locationId);
			ps.setInt(2, orderId);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}

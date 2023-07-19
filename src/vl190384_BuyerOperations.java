package rs.etf.sab.student;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import rs.etf.sab.operations.BuyerOperations;

public class vl190384_BuyerOperations implements BuyerOperations {

	private final String createBuyerQuery = "INSERT INTO [dbo].[Buyer] ([Name], [Credit], [IdCity]) "
			+ "VALUES (?, ?, ?)";
	private final String createOrderQuery = "INSERT INTO [dbo].[Order] ([IdBuyer]) VALUES (?)";
	private final String getCityQuery = "SELECT [IdCity] FROM [dbo].[Buyer] WHERE [IdBuyer] = ?";
	private final String getCreditQuery = "SELECT [Credit] FROM [dbo].[Buyer] WHERE [IdBuyer] = ?";
	private final String getOrdersQuery = "SELECT [IdOrder] FROM [dbo].[Order] WHERE [IdBuyer] = ?";
	private final String increaseCreditQuery = "SELECT [Credit] FROM [dbo].[Buyer] WHERE [IdBuyer] = ?";
	private final String setCityQuery = "UPDATE [dbo].[Buyer] SET [IdCity] = ? WHERE [IdBuyer] = ?";

	@Override
	public int createBuyer(String name, int cityId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(createBuyerQuery,
				PreparedStatement.RETURN_GENERATED_KEYS)) {
			ps.setString(1, name);
			ps.setBigDecimal(2, new BigDecimal(0));
			ps.setInt(3, cityId);
			ps.executeUpdate();
			ResultSet rs = ps.getGeneratedKeys();
			if (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public int createOrder(int buyerId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(createOrderQuery,
				PreparedStatement.RETURN_GENERATED_KEYS)) {
			ps.setInt(1, buyerId);
			ps.executeUpdate();
			ResultSet rs = ps.getGeneratedKeys();
			if (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public int getCity(int buyerId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getCityQuery)) {
			ps.setInt(1, buyerId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public BigDecimal getCredit(int buyerId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getCreditQuery)) {
			ps.setInt(1, buyerId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				return rs.getBigDecimal(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<Integer> getOrders(int buyerId) {
		List<Integer> res = new ArrayList<>();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getOrdersQuery)) {
			ps.setInt(1, buyerId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null) {
				while (rs.next()) {
					res.add(rs.getInt(1));
				}
				return res;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public BigDecimal increaseCredit(int buyerId, BigDecimal credit) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(increaseCreditQuery,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);) {
			ps.setInt(1, buyerId);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				BigDecimal updatedCredit = rs.getBigDecimal(1).add(credit);
				rs.updateBigDecimal(1, updatedCredit);
				rs.updateRow();
				return updatedCredit;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int setCity(int buyerId, int cityId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(setCityQuery)) {
			ps.setInt(1, cityId);
			ps.setInt(2, buyerId);
			if (ps.executeUpdate() != 0) {
				return 1;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

}

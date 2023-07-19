package rs.etf.sab.student;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import rs.etf.sab.operations.TransactionOperations;

public class vl190384_TransactionOperations implements TransactionOperations {

	private static final String buyerPaidQuery = "SELECT [Amount] FROM [dbo].[Transaction] "
			+ "WHERE [IdOrder] = ? AND [IdBuyer] IS NOT NULL AND [TimeOfExecution] IS NOT NULL";
	private static final String shopReceivedQuery = "SELECT [Amount] FROM [dbo].[Transaction] "
			+ "WHERE [IdOrder] = ? AND [IdShop] = ? AND [TimeOfExecution] IS NOT NULL";
	private static final String buyerSumQuery = "SELECT SUM([Amount]) FROM [dbo].[Transaction] "
			+ "WHERE [IdBuyer] = ? AND [TimeOfExecution] IS NOT NULL";
	private static final String shopSumQuery = "SELECT SUM([Amount]) FROM [dbo].[Transaction] "
			+ "WHERE [IdShop] = ? AND [TimeOfExecution] IS NOT NULL";

	private static final String systemProfitQuery = "  SELECT COALESCE(SUM(Term), 0) FROM "
			+ "	(SELECT CASE WHEN [IdShop] IS NULL THEN [Amount] ELSE -[Amount] END AS Term "
			+ "	FROM [dbo].[Transaction] WHERE [IdOrder] NOT IN " + "(SELECT [IdOrder] FROM [dbo].[Order] o "
			+ "	WHERE o.IdOrder = ANY "
			+ "(SELECT t.IdOrder FROM [dbo].[Transaction] t WHERE IdOrder = o.IdOrder AND t.TimeOfExecution IS NULL)"
			+ ")) AS Temp ";

	private static final String transactionTimeQuery = "SELECT [TimeOfExecution] FROM [dbo].[Transaction] "
			+ "WHERE [IdTransaction] = ? AND [TimeOfExecution] IS NOT NULL";
	private static final String transactionAmountQuery = "SELECT [Amount] FROM [dbo].[Transaction] "
			+ "WHERE [IdTransaction] = ? AND [TimeOfExecution] IS NOT NULL";
	private static final String orderBuyerTransactionQuery = "SELECT [IdTransaction] FROM [dbo].[Transaction] "
			+ "WHERE [IdOrder] = ? AND [IdBuyer] IS NOT NULL AND [TimeOfExecution] IS NOT NULL";
	private static final String orderShopTransactionQuery = "SELECT [IdTransaction] FROM [dbo].[Transaction] "
			+ "WHERE [IdOrder] = ? AND [IdShop] = ? AND [TimeOfExecution] IS NOT NULL";
	private static final String buyerTransactionsQuery = "SELECT [IdTransaction] FROM [dbo].[Transaction] "
			+ "WHERE [IdBuyer] = ? AND [TimeOfExecution] IS NOT NULL";
	private static final String shopTransactionsQuery = "SELECT [IdTransaction] FROM [dbo].[Transaction] "
			+ "WHERE [IdShop] = ? AND [TimeOfExecution] IS NOT NULL";

	@Override
	public BigDecimal getAmmountThatBuyerPayedForOrder(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(buyerPaidQuery)) {
			ps.setInt(1, orderId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				return rs.getBigDecimal(1).setScale(3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public BigDecimal getAmmountThatShopRecievedForOrder(int shopId, int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(shopReceivedQuery)) {
			ps.setInt(1, orderId);
			ps.setInt(2, shopId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				return rs.getBigDecimal(1).setScale(3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public BigDecimal getBuyerTransactionsAmmount(int buyerId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(buyerSumQuery)) {
			ps.setInt(1, buyerId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				BigDecimal retval = rs.getBigDecimal(1);
				if (!rs.wasNull())
					return retval.setScale(3);
				else
					return new BigDecimal(0).setScale(3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public BigDecimal getShopTransactionsAmmount(int shopId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(shopSumQuery)) {
			ps.setInt(1, shopId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				BigDecimal retval = rs.getBigDecimal(1);
				if (!rs.wasNull())
					return retval.setScale(3);
				else
					return new BigDecimal(0).setScale(3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public BigDecimal getSystemProfit() {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(systemProfitQuery)) {
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				BigDecimal retval = rs.getBigDecimal(1);
				if (!rs.wasNull())
					return retval.setScale(3);
				else
					return new BigDecimal(0).setScale(3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Calendar getTimeOfExecution(int transactionId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(transactionTimeQuery)) {
			ps.setInt(1, transactionId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				Calendar retval = Calendar.getInstance();
				Date dummy = rs.getDate(1);
				retval.setTime(dummy);
				if (!rs.wasNull()) {
					return retval;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public BigDecimal getTransactionAmount(int transactionId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(transactionAmountQuery)) {
			ps.setInt(1, transactionId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next())
				return rs.getBigDecimal(1).setScale(3);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int getTransactionForBuyersOrder(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(orderBuyerTransactionQuery)) {
			ps.setInt(1, orderId);
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
	public int getTransactionForShopAndOrder(int orderId, int shopId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(orderShopTransactionQuery)) {
			ps.setInt(1, orderId);
			ps.setInt(2, shopId);
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
	public List<Integer> getTransationsForBuyer(int buyerId) {
		List<Integer> res = new ArrayList<>();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(buyerTransactionsQuery)) {
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
	public List<Integer> getTransationsForShop(int shopId) {
		List<Integer> res = new ArrayList<>();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(shopTransactionsQuery)) {
			ps.setInt(1, shopId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null) {
				while (rs.next()) {
					res.add(rs.getInt(1));
				}
				if (res.size() == 0)
					return null;
				else
					return res;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

}

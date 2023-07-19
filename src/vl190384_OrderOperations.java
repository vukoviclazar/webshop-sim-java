package rs.etf.sab.student;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import rs.etf.sab.operations.OrderOperations;
import rs.etf.sab.student.GraphUtilities.Transition;

public class vl190384_OrderOperations implements OrderOperations {

	private static final String decreaseArticleCountInShopQuery = "SELECT [Count] FROM [dbo].[Article] WHERE [IdArticle] = ?";
	private static final String getArticleCountInOrderQuery = "SELECT [IdOrderHasArticle], [Count] FROM [dbo].[OrderHasArticle] WHERE [IdArticle] = ? AND  [IdOrder] = ?";
	private static final String insertArticleInOrderQuery = "INSERT INTO [dbo].[OrderHasArticle] ([Count], [IdArticle], [IdOrder]) VALUES (?, ?, ?)";
	private static final String deleteArticleInOrderQuery = "DELETE FROM [dbo].[OrderHasArticle] WHERE [IdArticle] = ? AND  [IdOrder] = ?";

	private static final String updateOrderToSentQuery = "UPDATE [dbo].[Order] "
			+ "SET [Status] = 'sent', [SentTime] = ?, [Location] = ? " + "WHERE [IdOrder] = ?";
	private final String getCitiesQuery = "SELECT [IdCity] FROM [dbo].[City]";
	private final String getConnectionsQuery = "SELECT [IdCity1], [IdCity2], [Distance] FROM [dbo].[Connection]";
	private final String getCitiesForShopsInOrderQuery = "SELECT DISTINCT s.[IdCity] FROM [dbo].[Shop] s "
			+ "JOIN [dbo].[Article] a ON a.IdShop = s.IdShop "
			+ "JOIN [dbo].[OrderHasArticle] oha ON oha.IdArticle = a.IdArticle "
			+ "JOIN [dbo].[Order] o ON o.IdOrder = oha.IdOrder " + "WHERE o.IdOrder = ?";
	private final String getAllShopsCitiesQuery = "SELECT DISTINCT s.[IdCity] FROM [dbo].[Shop] s ";

	private final String getBuyersCityFromOrder = "SELECT b.[IdCity] FROM [dbo].[Buyer] b "
			+ "JOIN [dbo].[Order] o ON o.IdBuyer = b.IdBuyer " + "WHERE o.IdOrder = ?";

	private static final String getBuyerQuery = "SELECT [IdBuyer] FROM [dbo].[Order] WHERE [IdOrder] = ?";
	private static final String getDiscountSumQuery = "SELECT [DiscountSum] FROM [dbo].[Order] WHERE [IdOrder] = ?";
	private static final String getFinalPriceQuery = "SELECT [FinalPrice] FROM [dbo].[Order] WHERE [IdOrder] = ?";
	private static final String getItemsQuery = "SELECT [IdOrderHasArticle] FROM [dbo].[OrderHasArticle] WHERE [IdOrder] = ?";
	private static final String getLocationQuery = "SELECT [Location] FROM [dbo].[Order] WHERE [IdOrder] = ?";
	private static final String getReceivedTimeQuery = "SELECT [ReceivedTime] FROM [dbo].[Order] WHERE [IdOrder] = ?";
	private static final String getSentTimeQuery = "SELECT [SentTime] FROM [dbo].[Order] WHERE [IdOrder] = ?";
	private static final String getStatusQuery = "SELECT [Status] FROM [dbo].[Order] WHERE [IdOrder] = ?";

	private vl190384_GeneralOperations generalOps;

	public vl190384_OrderOperations(vl190384_GeneralOperations generalOps) {
		this.generalOps = generalOps;
	}

	private boolean increaseArticleCountInShop(int articleId, int count) throws SQLException {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(decreaseArticleCountInShopQuery,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);) {
			ps.setInt(1, articleId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs.next() && rs.getInt(1) + count >= 0) {
				int updatedCount = rs.getInt(1) + count;
				rs.updateInt(1, updatedCount);
				rs.updateRow();
				return true;
			}
		}
		return false;
	}

	private int increaseArticleCountInOrder(int orderId, int articleId, int count) throws SQLException {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getArticleCountInOrderQuery,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);) {
			ps.setInt(1, articleId);
			ps.setInt(2, orderId);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				int updatedCount = rs.getInt(2) + count;
				int retval = rs.getInt(1);
				rs.updateInt(2, updatedCount);
				rs.updateRow();
				return retval;
			}
		}
		return -1;
	}

	private int insertArticleInOrder(int orderId, int articleId, int count) throws SQLException {
		try (PreparedStatement ps2 = DB.getInstance().getConnection().prepareStatement(insertArticleInOrderQuery,
				PreparedStatement.RETURN_GENERATED_KEYS)) {
			ps2.setInt(1, count);
			ps2.setInt(2, articleId);
			ps2.setInt(3, orderId);
			ps2.executeUpdate();
			ResultSet rs2 = ps2.getGeneratedKeys();
			if (rs2.next()) {
				return rs2.getInt(1);
			}
		}
		return -1;
	}

	@Override
	public int addArticle(int orderId, int articleId, int count) {
		boolean prevAutoComm = false;
		boolean prevAutoCommFetched = false;
		int retval = -1;
		try {
			prevAutoComm = DB.getInstance().getConnection().getAutoCommit();
			DB.getInstance().getConnection().setAutoCommit(false);
			prevAutoCommFetched = true;

			if (increaseArticleCountInShop(articleId, -count)) {
				retval = increaseArticleCountInOrder(orderId, articleId, count);
				if (retval == -1)
					retval = insertArticleInOrder(orderId, articleId, count);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (prevAutoCommFetched)
			try {
				DB.getInstance().getConnection().setAutoCommit(prevAutoComm);
			} catch (SQLException e) {
				e.printStackTrace();
			}

		return retval;
	}

	private int checkAdditionalDiscount(Date curDate, int orderId) throws SQLException {
		try (CallableStatement cs = DB.getInstance().getConnection()
				.prepareCall("{? = call F_CHECK_FOR_EXTRA_DISCOUNT(?, ?)}");) {
			cs.setDate(2, curDate);
			cs.setInt(3, orderId);
			cs.registerOutParameter(1, Types.INTEGER);
			cs.execute();
			return cs.getInt(1);
		}
	}

	private void calcPriceAndDiscount(int orderId, int isDiscountGranted) throws SQLException {
		try (CallableStatement cs = DB.getInstance().getConnection().prepareCall("{call SP_FINAL_PRICE(?, ?)}");) {
			cs.setInt(1, orderId);
			cs.setInt(2, isDiscountGranted);
			cs.execute();
		}
	}

	private void generateTransactions(Date curDate, int orderId) throws SQLException {
		try (CallableStatement cs = DB.getInstance().getConnection()
				.prepareCall("{call SP_GENERATE_TRANSACTIONS(?, ?)}");) {
			cs.setDate(1, curDate);
			cs.setInt(2, orderId);
			cs.execute();
		}
	}

	private int updateOrderToSent(Date curDate, int locationId, int orderId) throws SQLException {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(updateOrderToSentQuery)) {
			ps.setDate(1, curDate);
			ps.setInt(2, locationId);
			ps.setInt(3, orderId);
			if (ps.executeUpdate() != 0) {
				return 1;
			}
		}
		return -1;
	}

	private void getAllCities(Map<Integer, Integer> direct, Map<Integer, Integer> inverse) throws SQLException {
		direct.clear();
		inverse.clear();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getCitiesQuery)) {
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null) {
				int i = 0;
				while (rs.next()) {
					inverse.put(i, rs.getInt(1));
					direct.put(rs.getInt(1), i);
					i++;
				}
			}
		}
	}

	private int[][] generateGraph(Map<Integer, Integer> direct) throws SQLException {
		int[][] graph = null;
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getConnectionsQuery)) {
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null) {
				graph = new int[direct.size()][direct.size()];
				for (int i = 0; i < graph.length; i++)
					for (int j = 0; j < graph.length; j++)
						if (i == j)
							graph[i][j] = 0;
						else
							graph[i][j] = Integer.MAX_VALUE;
				int i, j, d;
				while (rs.next()) {
					i = direct.get(rs.getInt(1));
					j = direct.get(rs.getInt(2));
					d = rs.getInt(3);
					graph[i][j] = d;
					graph[j][i] = d;
				}
			}
		}
		return graph;
	}

	private void getCitiesForShopsInOrder(Set<Integer> cities, int orderId, Map<Integer, Integer> direct)
			throws SQLException {
		cities.clear();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getCitiesForShopsInOrderQuery)) {
			ps.setInt(1, orderId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null) {
				while (rs.next()) {
					cities.add(direct.get(rs.getInt(1)));
				}
			}
		}
	}

	private void getAllShopsCities(Set<Integer> cities, Map<Integer, Integer> direct) throws SQLException {
		cities.clear();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getAllShopsCitiesQuery)) {
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null) {
				while (rs.next()) {
					cities.add(direct.get(rs.getInt(1)));
				}
			}
		}
	}

	private int getBuyersCity(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getBuyersCityFromOrder)) {
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

	private void generateEvents(List<GraphUtilities.Transition> transitions, int orderId,
			Map<Integer, Integer> inverse) {
		for (Transition t : transitions) {
			Calendar curTime = generalOps.getCurrentTime();
			curTime.add(Calendar.DAY_OF_MONTH, t.getDistance());
			generalOps.registerEvent(new LocationUpdateEvent(curTime, orderId, inverse.get(t.getDestination())));
		}

		Transition t = transitions.get(transitions.size() - 1);
		Calendar curTime = generalOps.getCurrentTime();
		curTime.add(Calendar.DAY_OF_MONTH, t.getDistance());
		generalOps.registerEvent(new OrderCompleteEvent(curTime, orderId, new Date(curTime.getTimeInMillis())));
	}

	@Override
	public int completeOrder(int orderId) {
		boolean prevAutoComm = false;
		boolean prevAutoCommFetched = false;
		int retval = -1;
		try {
			prevAutoComm = DB.getInstance().getConnection().getAutoCommit();
			DB.getInstance().getConnection().setAutoCommit(false);
			prevAutoCommFetched = true;

			Date curDate = new Date(generalOps.getCurrentTime().getTimeInMillis());
			int isDiscountGranted = checkAdditionalDiscount(curDate, orderId);

			calcPriceAndDiscount(orderId, isDiscountGranted);
			generateTransactions(curDate, orderId);

			Map<Integer, Integer> direct = new HashMap<>();
			Map<Integer, Integer> inverse = new HashMap<>();
			Set<Integer> orderShopsCities = new HashSet<>();
			Set<Integer> allShopsCities = new HashSet<>();

			getAllCities(direct, inverse);
			getCitiesForShopsInOrder(orderShopsCities, orderId, direct);
			getAllShopsCities(allShopsCities, direct);

			int[][] graph = generateGraph(direct);

			int buyerCityId = direct.get(getBuyersCity(orderId));

			GraphUtilities gu = new GraphUtilities(graph, buyerCityId, allShopsCities, orderShopsCities);
			gu.floydWarshall();
			updateOrderToSent(curDate, inverse.get(gu.getClosestShop()), orderId);

			generateEvents(gu.getTransitionsToBuyer(), orderId, inverse);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (prevAutoCommFetched)
			try {
				DB.getInstance().getConnection().setAutoCommit(prevAutoComm);
			} catch (SQLException e) {
				e.printStackTrace();
			}

		return retval;

	}

	@Override
	public int getBuyer(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getBuyerQuery)) {
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
	public BigDecimal getDiscountSum(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getDiscountSumQuery)) {
			ps.setInt(1, orderId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				BigDecimal retval = rs.getBigDecimal(1);
				if (!rs.wasNull())
					return retval.setScale(3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public BigDecimal getFinalPrice(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getFinalPriceQuery)) {
			ps.setInt(1, orderId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				BigDecimal retval = rs.getBigDecimal(1);
				if (!rs.wasNull())
					return retval.setScale(3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<Integer> getItems(int orderId) {
		List<Integer> res = new ArrayList<>();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getItemsQuery)) {
			ps.setInt(1, orderId);
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
	public int getLocation(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getLocationQuery)) {
			ps.setInt(1, orderId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				int retval = rs.getInt(1);
				if (!rs.wasNull())
					return retval;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public Calendar getRecievedTime(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getReceivedTimeQuery)) {
			ps.setInt(1, orderId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				Calendar retval = Calendar.getInstance();
				Date dummy = rs.getDate(1);
				if (!rs.wasNull()) {
					retval.setTime(dummy);
					return retval;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Calendar getSentTime(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getSentTimeQuery)) {
			ps.setInt(1, orderId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				Calendar retval = Calendar.getInstance();
				Date dummy = rs.getDate(1);
				if (!rs.wasNull()) {
					retval.setTime(dummy);
					return retval;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String getState(int orderId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getStatusQuery)) {
			ps.setInt(1, orderId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				return rs.getString(1).trim();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	private int removeArticleAndReturnCount(int orderId, int articleId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getArticleCountInOrderQuery)) {
			ps.setInt(1, articleId);
			ps.setInt(2, orderId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			if (rs != null && rs.next()) {
				int retval = rs.getInt(1);

				try (PreparedStatement ps1 = DB.getInstance().getConnection()
						.prepareStatement(deleteArticleInOrderQuery)) {
					ps1.setInt(1, articleId);
					ps1.setInt(2, orderId);
					if (ps1.executeUpdate() > 0)
						return retval;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public int removeArticle(int orderId, int articleId) {
		boolean prevAutoComm = false;
		boolean prevAutoCommFetched = false;
		int retval = -1;
		try {
			prevAutoComm = DB.getInstance().getConnection().getAutoCommit();
			DB.getInstance().getConnection().setAutoCommit(false);
			prevAutoCommFetched = true;

			int count = removeArticleAndReturnCount(orderId, articleId);

			if (count != -1 && increaseArticleCountInShop(articleId, count)) {
				retval = 1;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (prevAutoCommFetched)
			try {
				DB.getInstance().getConnection().setAutoCommit(prevAutoComm);
			} catch (SQLException e) {
				e.printStackTrace();
			}

		return retval;
	}

}

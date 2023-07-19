package rs.etf.sab.student;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import rs.etf.sab.operations.ShopOperations;

public class vl190384_ShopOperations implements ShopOperations {

	private static String createShopQuery = "INSERT INTO [dbo].[Shop] ([Name], [IdCity]) "
			+ "VALUES (?, (SELECT [IdCity] FROM [dbo].[City] WHERE [Name] = ?))";
	private static String getArticleCountQuery = "SELECT [Count] FROM [dbo].[Article] WHERE [IdArticle] = ?";
	private static String getArticlesQuery = "SELECT [IdArticle] FROM [dbo].[Article] WHERE [IdShop] = ?";
	private static String getCityQuery = "SELECT [IdCity] FROM [dbo].[Shop] WHERE [IdShop] = ?";
	private static String getDiscountQuery = "SELECT [Discount] FROM [dbo].[Shop] WHERE [IdShop] = ?";
	private static String increaseArticleCountQuery = "SELECT [Count] FROM [dbo].[Article] WHERE [IdArticle] = ?";
	private static String setCityQuery = "UPDATE [dbo].[Shop] "
			+ "SET [IdCity] = (SELECT [IdCity] FROM [dbo].[City] WHERE [Name] = ?) " + "WHERE [IdShop] = ?";
	private static String setDiscountQuery = "UPDATE [dbo].[Shop] SET [Discount] = ? WHERE [IdShop] = ?";

	@Override
	public int createShop(String name, String cityName) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(createShopQuery,
				PreparedStatement.RETURN_GENERATED_KEYS)) {
			ps.setString(1, name);
			ps.setString(2, cityName);
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
	public int getArticleCount(int articleId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getArticleCountQuery)) {
			ps.setInt(1, articleId);
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
	public List<Integer> getArticles(int shopId) {
		List<Integer> res = new ArrayList<>();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getArticlesQuery)) {
			ps.setInt(1, shopId);
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
	public int getCity(int shopId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getCityQuery)) {
			ps.setInt(1, shopId);
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
	public int getDiscount(int shopId) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getDiscountQuery)) {
			ps.setInt(1, shopId);
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
	public int increaseArticleCount(int articleId, int increment) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(increaseArticleCountQuery,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);) {
			ps.setInt(1, articleId);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				int updatedCount = rs.getInt(1) + increment;
				rs.updateInt(1, updatedCount);
				rs.updateRow();
				return updatedCount;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public int setCity(int shopId, String cityName) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(setCityQuery)) {
			ps.setString(1, cityName);
			ps.setInt(2, shopId);
			if (ps.executeUpdate() != 0) {
				return 1;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public int setDiscount(int shopId, int discountPercentage) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(setDiscountQuery)) {
			ps.setInt(1, discountPercentage);
			ps.setInt(2, shopId);
			if (ps.executeUpdate() != 0) {
				return 1;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

}

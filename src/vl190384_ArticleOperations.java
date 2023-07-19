package rs.etf.sab.student;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import rs.etf.sab.operations.ArticleOperations;

public class vl190384_ArticleOperations implements ArticleOperations {

	private static final String createArticleQuery = "INSERT INTO [dbo].[Article] " + "([Name], [Price], [IdShop]) "
			+ "VALUES (?, ?, ?);";

	@Override
	public int createArticle(int shopId, String articleName, int articlePrice) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(createArticleQuery,
				PreparedStatement.RETURN_GENERATED_KEYS)) {
			ps.setString(1, articleName);
			ps.setInt(2, articlePrice);
			ps.setInt(3, shopId);
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

}

package rs.etf.sab.student;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import rs.etf.sab.operations.CityOperations;

public class vl190384_CityOperations implements CityOperations {

	private final String createCityQuery = "INSERT INTO [dbo].[City] ([Name]) VALUES (?)";
	private final String connectCitiesQuery = "INSERT INTO [dbo].[Connection] ([Distance] ,[IdCity1] ,[IdCity2]) "
			+ "VALUES (?, ?, ?)";
	private final String getCitiesQuery = "SELECT [IdCity] FROM [dbo].[City]";
	private final String getConnectedCitiesQuery = "SELECT [IdCity2] FROM [dbo].[Connection] WHERE [IdCity1] = ? "
			+ "UNION " + "SELECT [IdCity1] FROM [dbo].[Connection] WHERE [IdCity2] = ?";
	private final String getShopsQuery = "SELECT [IdShop] FROM [dbo].[Shop] WHERE [IdCity] = ?";

	@Override
	public int connectCities(int cityId1, int cityId2, int distance) {
		if (cityId1 > cityId2) {
			int temp = cityId1;
			cityId1 = cityId2;
			cityId2 = temp;
		} else if (cityId1 == cityId2)
			return -1;
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(connectCitiesQuery,
				PreparedStatement.RETURN_GENERATED_KEYS)) {
			ps.setInt(1, distance);
			ps.setInt(2, cityId1);
			ps.setInt(3, cityId2);
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
	public int createCity(String name) {
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(createCityQuery,
				PreparedStatement.RETURN_GENERATED_KEYS)) {
			ps.setString(1, name);
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
	public List<Integer> getCities() {
		List<Integer> res = new ArrayList<>();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getCitiesQuery)) {
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
	public List<Integer> getConnectedCities(int cityId) {
		List<Integer> res = new ArrayList<>();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getConnectedCitiesQuery)) {
			ps.setInt(1, cityId);
			ps.setInt(2, cityId);
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
	public List<Integer> getShops(int cityId) {
		List<Integer> res = new ArrayList<>();
		try (PreparedStatement ps = DB.getInstance().getConnection().prepareStatement(getShopsQuery)) {
			ps.setInt(1, cityId);
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

}

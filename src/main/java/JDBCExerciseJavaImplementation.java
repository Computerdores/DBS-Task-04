import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

@ChosenImplementation(true)
public class JDBCExerciseJavaImplementation implements JDBCExercise {

	Logger logger = Logger.getLogger(this.getClass().getSimpleName());

	@Override
	public Connection createConnection(@NotNull ConnectionConfig config) throws SQLException {
		return DriverManager.getConnection(
				String.format("jdbc:postgresql://%s:%d/%s",
					config.getHost(), config.getPort(), config.getDatabase()),
				config.getUsername(),
				config.getPassword()
		);
	}

	@Override
	public List<Movie> queryMovies(
		@NotNull Connection connection,
		@NotNull String keywords
	) throws SQLException {
		logger.info(keywords);
		List<Movie> movies = new ArrayList<>();

		PreparedStatement movieQuery = connection.prepareStatement(
				"SELECT tconst, title, year, genres FROM tmovies WHERE title LIKE ? ORDER BY title ASC, year ASC;");
		movieQuery.setString(1, "%"+keywords+"%");

		ResultSet rs = movieQuery.executeQuery();

		while (rs.next()) {
			String   tconst = rs.getString("tconst");
			String    title = rs.getString("title");
			int        year = rs.getInt("year");
			String[] genres = (String[])rs.getArray("genres").getArray();
			movies.add(new Movie(tconst, title, year, new HashSet<>(List.of(genres))));
		}

		return movies;
	}

	@Override
	public List<Actor> queryActors(
		@NotNull Connection connection,
		@NotNull String keywords
	) throws SQLException {
		logger.info(keywords);
		List<Actor> actors = new ArrayList<>();

		throw new UnsupportedOperationException("Not yet implemented");
	}
}

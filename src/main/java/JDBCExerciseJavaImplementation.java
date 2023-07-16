import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
				"SELECT tconst, title, year, genres, ARRAY_AGG(primaryname) as actors " +
						"FROM tmovies LEFT OUTER JOIN (" +
							"SELECT tconst AS ptconst, nconst AS pnconst, category " +
							"FROM tprincipals " +
							"WHERE category = 'actor' OR category = 'actress'" +
						") AS tp ON tconst = ptconst LEFT OUTER JOIN nbasics ON nconst = pnconst " +
						"WHERE title LIKE ? " +
						"GROUP BY tconst, title, year, genres " +
						"ORDER BY title ASC, year ASC;");
		movieQuery.setString(1, "%"+keywords+"%");

		ResultSet rs = movieQuery.executeQuery();

		while (rs.next()) {
			String   tconst = rs.getString("tconst");
			String    title = rs.getString("title");
			int        year = rs.getInt("year");
			String[] genres = (String[])rs.getArray("genres").getArray();
			String[] actors = (String[])rs.getArray("actors").getArray();
			Movie mov = new Movie(tconst, title, year, new HashSet<>(List.of(genres)));
			Arrays.sort(actors);
			for (String s : actors) {
				if (s != null) {
					mov.actorNames.add(s);
				}
			}
			movies.add(mov);
		}

		movieQuery.close();

		return movies;
	}

	@Override
	public List<Actor> queryActors(@NotNull Connection connection, @NotNull String keywords) throws SQLException {
		//////// RETRIEVE ACTORS ////////
		PreparedStatement actorQuery = connection.prepareStatement("""
			WITH unsorted AS (
				SELECT nconst, primaryname, ARRAY_AGG(tconst) AS movies
				FROM tprincipals NATURAL JOIN nbasics
				WHERE (category = 'actor' OR category = 'actress') AND primaryname LIKE ?
				GROUP BY nconst, primaryname
			)
			SELECT nconst, primaryname, ARRAY_LENGTH(movies, 1) AS movie_count
			FROM unsorted
			ORDER BY movie_count DESC, primaryname ASC
			LIMIT 5;
		""");
		actorQuery.setString(1, "%"+keywords+"%");
		ResultSet result = actorQuery.executeQuery();

		List<Actor> actors = new ArrayList<>();

		while (result.next())
		{
			String nconst = result.getString("nconst");
			String primaryname = result.getString("primaryname");

			Actor actor = new Actor(nconst, primaryname);

			actors.add(actor);
		}

		actorQuery.close();

		//////// RETRIEVE RECENT MOVIES FOR EACH ACTOR ////////

		PreparedStatement movieQuery = connection.prepareStatement("""
				SELECT title
				FROM tprincipals NATURAL JOIN tmovies
				WHERE nconst = ? AND (category = 'actor' OR category = 'actress') AND year > 0
				ORDER BY year DESC, title ASC
				LIMIT 5;
		""");

		PreparedStatement costarQuery = connection.prepareStatement("""
				WITH all_movies AS (
					SELECT tconst
					FROM tprincipals
					WHERE nconst = ? AND (category = 'actor' OR category = 'actress')
				)
				
				SELECT primaryname, COUNT(*) AS mov_count
				FROM all_movies NATURAL JOIN tprincipals NATURAL JOIN nbasics NATURAL JOIN tmovies
				WHERE nconst != ? AND (category = 'actor' OR category = 'actress')
				GROUP BY nconst, primaryname
				ORDER BY mov_count DESC, primaryname ASC
				LIMIT 5;
		""");

		for(Actor actor : actors) {
			movieQuery.setString(1, actor.nConst);
			result = movieQuery.executeQuery();

			while (result.next()) {
				actor.playedIn.add(result.getString("title"));
			}

			//////// RETRIEVE MOST PLAYED WITH ////////

			costarQuery.setString(1, actor.nConst);
			costarQuery.setString(2, actor.nConst);
			result = costarQuery.executeQuery();

			while (result.next()) {
				actor.costarNameToCount.put(result.getString("primaryname"), result.getInt("mov_count"));
			}
		}

		movieQuery.close();
		costarQuery.close();

		return actors;
	}
}
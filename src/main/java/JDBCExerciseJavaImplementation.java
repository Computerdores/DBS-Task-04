import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.ActorCounter;
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
	public List<Actor> queryActors(@NotNull Connection connection, @NotNull String keywords) throws SQLException
	{
		//////// RETRIEVE ACTORS ////////
		PreparedStatement actorQuery =
				connection.prepareStatement("SELECT " +
						"nconst, primaryname " +
						"FROM tprincipals NATURAL JOIN nbasics " +
						"WHERE (category = 'actor' OR category = 'actress') AND ARRAY_LENGTH(knownfortitles, 1) > 0 " +
						"AND primaryname LIKE ? " +
						"GROUP BY nconst, primaryname, knownfortitles " +
						"ORDER BY ARRAY_LENGTH(knownfortitles, 1) DESC, primaryname ASC " +
						"LIMIT 5;");
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

		//////// RETRIEVE RECENT MOVIES FOR EACH ACTOR ////////

		for(Actor actor : actors)
		{
			PreparedStatement movieQuery = connection.prepareStatement("SELECT title FROM tprincipals NATURAL JOIN tmovies WHERE nconst = ? AND (category = 'actor' OR category = 'actress') AND year > 0 ORDER BY year DESC, title ASC LIMIT 5;");

			movieQuery.setString(1, actor.nConst);
			result = movieQuery.executeQuery();

			while (result.next())
			{
				actor.playedIn.add(result.getString("title"));
			}

			PreparedStatement coStarAllMoviesQuery = connection.prepareStatement("SELECT tconst FROM tprincipals WHERE nconst = ? AND (category = 'actor' OR category = 'actress');");
			coStarAllMoviesQuery.setString(1, actor.nConst);
			ResultSet allMovies = coStarAllMoviesQuery.executeQuery();

			List<ActorCounter> playedWith = new ArrayList<ActorCounter>();

			while (allMovies.next())
			{
				PreparedStatement getAllCoActors = connection.prepareStatement("SELECT primaryname FROM nbasics NATURAL JOIN tprincipals WHERE tconst = ?;");
				getAllCoActors.setString(1, allMovies.getString("tconst"));
				ResultSet allActorsFromMovies = getAllCoActors.executeQuery();

				while(allActorsFromMovies.next())
				{
					String name = allActorsFromMovies.getString("primaryname");
					boolean contained = false;
					int index = 0;

					for(ActorCounter ac : playedWith)
					{
						if (ac.name == name)
						{
							index = playedWith.indexOf(ac);
							contained = true;
							break;
						}
					}

					if(contained)
					{
						playedWith.get(index).increaseCount();
					}
					else
					{
						playedWith.add(new ActorCounter(name));
					}

				}
			}

			playedWith.sort(ActorCounter::compareTo);

			for(ActorCounter ac : playedWith)
			{
				actor.costarNameToCount.put(ac.name, ac.count);
			}


		}




		return actors;
	}
}
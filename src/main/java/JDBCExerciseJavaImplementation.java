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

		//////// RETRIEVE RECENT MOVIES FOR EACH ACTOR ////////

		for(Actor actor : actors)
		{
			PreparedStatement movieQuery = connection.prepareStatement("""
				SELECT title
				FROM tprincipals NATURAL JOIN tmovies
				WHERE nconst = ? AND (category = 'actor' OR category = 'actress') AND year > 0
				ORDER BY year DESC, title ASC
				LIMIT 5;
			""");

			movieQuery.setString(1, actor.nConst);
			result = movieQuery.executeQuery();

			while (result.next())
			{
				actor.playedIn.add(result.getString("title"));
			}

			//////// RETRIEVE MOST PLAYED WITH ////////

			PreparedStatement coStarAllMoviesQuery = connection.prepareStatement("SELECT tconst FROM tprincipals WHERE nconst = ? AND (category = 'actor' OR category = 'actress');");
			coStarAllMoviesQuery.setString(1, actor.nConst);
			ResultSet allMovies = coStarAllMoviesQuery.executeQuery();

			List<ActorCounter> playedWith = new ArrayList<ActorCounter>();

			int accountedMovies = 0;

			while (allMovies.next())//for all movies the actor played in
			{
				accountedMovies++;

				PreparedStatement getAllCoActors = connection.prepareStatement("SELECT primaryname FROM nbasics NATURAL JOIN tprincipals WHERE tconst = ? AND (category = 'actor' OR category = 'actress');");
				getAllCoActors.setString(1, allMovies.getString("tconst"));
				ResultSet allActorsFromMovies = getAllCoActors.executeQuery();

				int accountedActors = 0;

				while(allActorsFromMovies.next())//for all actors who played in that movie
				{
					accountedActors++;
					String coName = allActorsFromMovies.getString("primaryname");
					boolean contained = false;

					for(ActorCounter ac : playedWith)//for all actors that are already being counted
					{
						if (ac.name == coName)//actor is contained
						{
							ac.increaseCount();
							contained = true;
							System.out.printf(coName + ": " + ac.count);
							break;
						}
					}

					if(!contained)//actor is not contained
					{
						playedWith.add(new ActorCounter(coName));
					}
				}
			}

			playedWith.get(0).count = accountedMovies;

			playedWith.sort(ActorCounter::compareToActor);


			for(ActorCounter ac : playedWith)
			{
				actor.costarNameToCount.put(ac.name, ac.count);
			}


		}

		return actors;
	}
}
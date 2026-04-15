using System.Globalization;
using System.Text.RegularExpressions;
using MoviesDatabaseChat.Entities;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;


namespace MoviesDatabaseChat
{
    internal static class DatabaseBootstrapper
    {
        public const string AiAgentIdentifier = "movie-recommender";

        public static async Task<bool> CreateDatabaseAsync(IDocumentStore store, Action<string> log, bool smallDb)
        {
            if (await DatabaseExistsAsync(store, store.Database)) // db exists -> don't create it again
                return false;
            
            log($"Creating database '{store.Database}'");
            
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database)));
            
            log("Initialize Movies Database:");
            
            await AddMoviesAsync(store, log);
            await AddRatingsAndUsersAsync(store, log, smallDb);
            await AddTagsAsync(store, log);
            
            log("Creating Indexes");
            await new Ratings_ByMovie_Stats().ExecuteAsync(store);
            await new Ratings_ByUser_LastRates().ExecuteAsync(store);
            if (smallDb == false)
                await new UserTagAffinity().ExecuteAsync(store);
            await new UserGenreAffinity().ExecuteAsync(store);
            await new MovieStats_ByVector_GenresTags_AndAverageRating_AndViews().ExecuteAsync(store);
            await new Ratings_ByUser_And_Movie().ExecuteAsync(store);
            await new MovieStats_ByGenres_Title_Tags_AverageRating_AndViews().ExecuteAsync(store);

            await ConfigAiAgentAsync(store, log, smallDb);

            log("Finished initializing the database!");

            return true;
        }

        private static async Task<bool> DatabaseExistsAsync(IDocumentStore store, string database)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
            return record != null;
        }


        // add docs funcs

        private static async Task AddRatingsAndUsersAsync(IDocumentStore store, Action<string> log, bool smallDb)
        {
            log("Ratings");
            var usersToCreate = new Dictionary<string, HashSet<string>>();
            int count = 0;

            for (int i = 1; i <= 10; i++)
            {

                using var reader = new StreamReader(Path.Combine("Csvs", $"ratings{i}.csv"));
                using var csv = new CsvHelper.CsvReader(reader, CultureInfo.InvariantCulture);

                using (var bulkInsert = store.BulkInsert())
                {
                    foreach (var row in csv.GetRecords<RatingCsvRow>())
                    {
                        var rating = row.ToRating();
                        if (rating == null)
                            continue;

                        if (usersToCreate.TryGetValue(rating.UserId, out var moviesList) == false || moviesList == null)
                            usersToCreate[rating.UserId] = new HashSet<string>();

                        usersToCreate[rating.UserId].Add(rating.MovieId);

                        await bulkInsert.StoreAsync(rating);
                        count++;

                        if (count % 500_000 == 0)
                        {
                            log($"Saved {count} ratings...");
                        }

                        if (smallDb && count >= 1_500_000)
                            break;
                    }
                }
                if (smallDb && count >= 1_500_000)
                    break;

            }

            log($"Done! Total saved: {count}");


            log("Users");
            count = 0;

            Random random = new Random();

            using (var bulkInsert = store.BulkInsert())
            {
                foreach (var kvp in usersToCreate)
                {
                    await bulkInsert.StoreAsync(new User
                    {
                        Id = kvp.Key,
                        Name = israeliFullNames[random.Next(israeliFullNames.Length)],
                        WatchedMovies = kvp.Value
                    });
                    count++;

                    if (count % 100_000 == 0)
                    {
                        log($"Saved {count} users...");
                    }
                }
            }

            log($"Done! Total saved: {count}");
        }

        private static async Task AddMoviesAsync(IDocumentStore store, Action<string> log)
        {
            log("Movies");

            using var reader = new StreamReader(Path.Combine("Csvs", "movies.csv"));
            using var csv = new CsvHelper.CsvReader(reader, CultureInfo.InvariantCulture);

            int count = 0;

            using (var bulkInsert = store.BulkInsert())
            {
                foreach (var row in csv.GetRecords<MovieCsvRow>())
                {
                    var movie = row.ToMovie();
                    if (movie == null)
                        continue;

                    await bulkInsert.StoreAsync(movie);
                    count++;

                    if (count % 10_000 == 0)
                    {
                        log($"Saved {count} movies...");
                    }
                }
            }

            log($"Done! Total saved: {count}");
        }

        private static async Task AddTagsAsync(IDocumentStore store, Action<string> log)
        {
            log("Tags");

            using var reader = new StreamReader(Path.Combine("Csvs", "tags.csv"));
            using var csv = new CsvHelper.CsvReader(reader, CultureInfo.InvariantCulture);

            int count = 0;

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

                foreach (var row in csv.GetRecords<TagCsvRow>())
                {
                    count++;
                    var movieId = "Movies/" + row.movieId;
                    var movie = await session.LoadAsync<Movie>(movieId);
                    if (movie == null)
                    {
                        // log($"{movieId} doesn't exist");
                        continue;
                    }

                    if (movie.Tags == null)
                        movie.Tags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                    movie.Tags.Add(row.tag);

                    if (count % 100_000 == 0)
                    {
                        await session.SaveChangesAsync();
                        log($"Saved {count} tags...");
                    }
                }

                await session.SaveChangesAsync();
            }
        }


        // create agent

        public static async Task ConfigAiAgentAsync(IDocumentStore store, Action<string> log, bool smallDb)
        {
            log("Creating Ai Agent");

            var conn = CreateAiConnectionString();
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(conn));

            var systemPrompt =
                "You are a movie recommender AI agent. You can query a RavenDB database to learn a user's taste and recommend unwatched movies. " +
                "Use the provided tools only. When listing movies, include their MovieId. " +
                "Each movie has genres, and can have user-provided tags and user ratings. " +
                "The only valid genres are: Action, Adventure, Animation, Children’s, Comedy, Crime, Documentary, Drama, Fantasy, Film-Noir, Horror, Musical, Mystery, Romance, Sci-Fi, Thriller, War, Western. " +
                "Each user has a Watched List and can rate movies or add tags.";

            var queryTools = new List<AiAgentToolQuery>()
            {
                new AiAgentToolQuery
                {
                    Name = "GetUserProfile",
                    Description = "Get the profile details of a specific user, including its 'WatchedMovies' list",
                    Query = "from Users " +
                            "where id() = $userId",
                    ParametersSampleObject = "{}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetUserLastRatings",
                    Description = "Get the last ratings made by a user for all the movies in its 'WatchedMovies' list",
                    Query = "from index 'Ratings/ByUser/LastRates' " +
                            "where UserId == $userId " +
                            "select LastRates",
                    ParametersSampleObject = "{}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetUserAffinitiesByGenres",
                    Description =
                        "Get user genre affinities sorted by score, ordered by average Score (rating) - descending",
                    Query = "from index 'UserGenreAffinity' " +
                            "where UserId = $userId " +
                            "order by Score desc " +
                            "select Genre, Score, Count " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"skip\": 0, \"pageSize\": 10}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetMovieStatsByAverageRatingDesc",
                    Description =
                        "Retrieves aggregated statistics for the specified movies, including their average rating (score), and returns the results ordered by average rating in descending order.",
                    Query = "from index 'Ratings/ByMovie/Stats' " +
                            "where MovieId in ($movieIds) " +
                            "order by AverageRating as double desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"movieIds\": [\"Movies/1\", \"Movies/2\"], \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetMovieStatsByAverageRatingAsc",
                    Description =
                        "Retrieves aggregated statistics for the specified movies, including their average rating (score), and returns the results ordered by average rating in ascending order.",
                    Query = "from index 'Ratings/ByMovie/Stats' " +
                            "where MovieId in ($movieIds) " +
                            "order by AverageRating as double asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"movieIds\": [\"Movies/1\", \"Movies/2\"], \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetMovieStatsByAverageViewsDesc",
                    Description =
                        "Retrieves aggregated statistics for the specified movies, including their average rating (score), and returns the results ordered by number of views in descending order.",
                    Query = "from index 'Ratings/ByMovie/Stats' " +
                            "where MovieId in ($movieIds) " +
                            "order by Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"movieIds\": [\"Movies/1\", \"Movies/2\"], \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetMovieStatsByAverageViewsAsc",
                    Description =
                        "Retrieves aggregated statistics for the specified movies, including their average rating (score), and returns the results ordered by  number of views  in ascending order.",
                    Query = "from index 'Ratings/ByMovie/Stats' " +
                            "where MovieId in ($movieIds) " +
                            "order by Views as long asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"movieIds\": [\"Movies/1\", \"Movies/2\"], \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetUserLastRatingsByMovieIds",
                    Description = "Get the user ratings (scores) for specific movies by movieIds list",
                    Query = "from index 'Ratings/ByUser/And/Movie' where UserId = $userId and MovieId in ($movieIds)",
                    ParametersSampleObject = "{\"movieIds\": [\"Movies/1\", \"Movies/2\"]}"
                },

                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTagsGenresVectorsAverageRatingDesc",
                    Description =
                        "Search movies candidates using vector search (vector similarity) on tags and genres, ordered by average rating in descending order",
                    Query = "from index 'MovieStats/ByVector/GenresTags/AndAverageRating/AndViews' as i " +
                            "where vector.search(i.TagsVector, $tagsQuery, 0.95, 500) or vector.search(i.GenresVector, $genresQuery, 0.85, 500) " +
                            "order by i.AverageRating as double desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"tagsQuery\": \"search for scary movies\", \"genresQuery\": \"search for movies of the genre 'horror'\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTagsGenresVectorsAverageRatingAsc",
                    Description =
                        "Search movies candidates using vector search (vector similarity) on tags and genres, ordered by average rating in ascending order",
                    Query = "from index 'MovieStats/ByVector/GenresTags/AndAverageRating/AndViews' as i " +
                            "where vector.search(i.TagsVector, $tagsQuery, 0.95, 500) or vector.search(i.GenresVector, $genresQuery, 0.85, 500) " +
                            "order by i.AverageRating as double asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"tagsQuery\": \"search for scary movies\", \"genresQuery\": \"search for movies of the genre 'horror'\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTagsGenresVectorsViewsAsc",
                    Description =
                        "Search movies candidates using vector search (vector similarity) on tags and genres, ordered by views in ascending order",
                    Query = "from index 'MovieStats/ByVector/GenresTags/AndAverageRating/AndViews' as i " +
                            "where vector.search(i.TagsVector, $tagsQuery, 0.95, 500) or vector.search(i.GenresVector, $genresQuery, 0.85, 500) " +
                            "order by i.Views as long asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"tagsQuery\": \"search for scary movies\", \"genresQuery\": \"search for movies of the genre 'horror'\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTagsGenresVectorsViewsDesc",
                    Description =
                        "Search movies using vector search (vector similarity) on tags and genres, ordered by views in descending order",
                    Query = "from index 'MovieStats/ByVector/GenresTags/AndAverageRating/AndViews' as i " +
                            "where vector.search(i.TagsVector, $tagsQuery, 0.95, 500) or vector.search(i.GenresVector, $genresQuery, 0.85, 500) " +
                            "order by i.Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"tagsQuery\": \"search for scary movies\", \"genresQuery\": \"search for movies of the genre 'horror'\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTagsVectorAverageRatingDesc",
                    Description =
                        "Search movies using vector similarity on tags only, ordered by average rating in descending order",
                    Query = "from index 'MovieStats/ByVector/GenresTags/AndAverageRating/AndViews' as i " +
                            "where vector.search(i.TagsVector, $tagsQuery, 0.95, 500) " +
                            "order by i.AverageRating as double desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"tagsQuery\": \"search for scary movies\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTagsVectorAverageRatingAsc",
                    Description =
                        "Search movies using vector similarity on tags only, ordered by average rating in ascending order",
                    Query = "from index 'MovieStats/ByVector/GenresTags/AndAverageRating/AndViews' as i " +
                            "where vector.search(i.TagsVector, $tagsQuery, 0.95, 500) " +
                            "order by i.AverageRating as double asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"tagsQuery\": \"search for scary movies\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTagsVectorViewsAsc",
                    Description =
                        "Search movies using vector similarity on tags only, ordered by views in ascending order",
                    Query = "from index 'MovieStats/ByVector/GenresTags/AndAverageRating/AndViews' as i " +
                            "where vector.search(i.TagsVector, $tagsQuery, 0.95, 500) " +
                            "order by i.Views as long asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"tagsQuery\": \"search for scary movies\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTagsVectorViewsDesc",
                    Description =
                        "Search movies using vector similarity on tags only, ordered by views in descending order",
                    Query = "from index 'MovieStats/ByVector/GenresTags/AndAverageRating/AndViews' as i " +
                            "where vector.search(i.TagsVector, $tagsQuery, 0.95, 500) " +
                            "order by i.Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject =
                        "{\"tagsQuery\": \"search for scary movies\", \"skip\": 0, \"pageSize\": 5}"
                },

                new AiAgentToolQuery
                {
                    Name = "GetMovieStatsByGenreOrderByAverageRatingDesc",
                    Description =
                        "Retrieves aggregated statistics for movies within a specified genre, including their average rating (score), and returns the results ordered by average rating in descending order. Supports pagination via skip and pageSize.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where Genres = $genre " +
                            "order by AverageRating as double desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"genre\": \"Action\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetMovieStatsByGenreOrderByAverageRatingAsc",
                    Description =
                        "Retrieves aggregated statistics for movies within a specified genre, including their average rating (score), and returns the results ordered by average rating in ascending order. Supports pagination via skip and pageSize.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where Genres = $genre " +
                            "order by AverageRating as double asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"genre\": \"Action\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetMovieStatsByGenreOrderByViewsDesc",
                    Description =
                        "Retrieves aggregated statistics for movies within a specified genre, including their average rating (score), and returns the results ordered by views number in descending order. Supports pagination via skip and pageSize.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where Genres = $genre " +
                            "order by Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"genre\": \"Action\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetMovieStatsByGenreOrderByViewsAsc",
                    Description =
                        "Retrieves aggregated statistics for movies within a specified genre, including their average rating (score), and returns the results views number rating in ascending order. Supports pagination via skip and pageSize.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where Genres = $genre " +
                            "order by Views as long asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"genre\": \"Action\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTitleOrderByAverageRatingDesc",
                    Description =
                        "Search for movies whose titles contain the specified text, returning aggregated stats ordered by average rating (score) in descending order. Supports pagination via skip and pageSize.",
                    Query =
                        "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                        "where search(Title, $text) " +
                        "order by AverageRating as double desc " +
                        "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"rex\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTitleOrderByAverageRatingAsc",
                    Description =
                        "Search for movies whose titles contain the specified text, returning aggregated stats ordered by average rating (score) in ascending order. Supports pagination via skip and pageSize.",
                    Query =
                        "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                        "where search(Title, $text) " +
                        "order by AverageRating as double asc " +
                        "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"rex\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTitleOrderByViewsDesc",
                    Description =
                        "Search for movies whose titles contain the specified text, returning aggregated stats ordered by views in descending order. Supports pagination via skip and pageSize.",
                    Query =
                        "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                        "where search(Title, $text) " +
                        "order by Views as long desc " +
                        "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"rex\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTitleOrderByViewsAsc",
                    Description =
                        "Search for movies whose titles contain the specified text, returning aggregated stats ordered by views in ascending order. Supports pagination via skip and pageSize.",
                    Query =
                        "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                        "where search(Title, $text) " +
                        "order by Views as long asc " +
                        "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"rex\", \"skip\": 0, \"pageSize\": 5}"
                },

                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTagsOrderByAverageRatingDesc",
                    Description =
                        "Search for movies whose tags contain the specified text, returning aggregated stats ordered by average rating (score) in descending order. Supports pagination via skip and pageSize.",
                    Query =
                        "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                        "where search(Tags, $text) " +
                        "order by AverageRating as double desc " +
                        "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"rex\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTagsOrderByAverageRatingAsc",
                    Description =
                        "Search for movies whose tags contain the specified text, returning aggregated stats ordered by average rating (score) in ascending order. Supports pagination via skip and pageSize.",
                    Query =
                        "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                        "where search(Tags, $text) " +
                        "order by AverageRating as double asc " +
                        "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"rex\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTagsOrderByViewsDesc",
                    Description =
                        "Search for movies whose tags contain the specified text, returning aggregated stats ordered by views in descending order. Supports pagination via skip and pageSize.",
                    Query =
                        "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                        "where search(Tags, $text) " +
                        "order by Views as long desc " +
                        "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"rex\", \"skip\": 0, \"pageSize\": 5}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTagsOrderByViewsAsc",
                    Description =
                        "Search for movies whose tags contain the specified text, returning aggregated stats ordered by views in ascending order. Supports pagination via skip and pageSize.",
                    Query =
                        "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                        "where search(Tags, $text) " +
                        "order by Views as long asc " +
                        "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"rex\", \"skip\": 0, \"pageSize\": 5}"
                },


                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTitleOrTagsOrderByAverageRatingDesc",
                    Description =
                        "Search for movies where the title OR tags contain the given text, ordered by average rating descending and then views descending.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where search(Title, $text) or search(Tags, $text) " +
                            "order by AverageRating as double desc, Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"avengers\", \"skip\": 0, \"pageSize\": 10}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMovieStatsByTitleOrTagsOrderByAverageRatingAsc",
                    Description =
                        "Search for movies where the title OR tags contain the given text, ordered by average rating ascending and then views descending.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where search(Title, $text) or search(Tags, $text) " +
                            "order by AverageRating as double asc, Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\": \"avengers\", \"skip\": 0, \"pageSize\": 10}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetTopRatedWithQualityThreshold",
                    Description = "Retrieve top-rated movies that meet minimum views and rating thresholds, ordered by average rating descending and then views descending.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where Views >= $minViews and AverageRating >= $minRating " +
                            "order by AverageRating as double desc, Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"minViews\": 100, \"minRating\": 3.8, \"skip\": 0, \"pageSize\": 10}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetTopRatedWithQualityThresholdAsc",
                    Description = "Retrieve movies that meet minimum views and rating thresholds, ordered by average rating ascending and then views descending.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where Views >= $minViews and AverageRating >= $minRating " +
                            "order by AverageRating as double asc, Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"minViews\": 100, \"minRating\": 3.8, \"skip\": 0, \"pageSize\": 10}"
                },

                new AiAgentToolQuery
                {
                    Name = "GetByGenresInListOrderByAverageRatingDesc",
                    Description = "Retrieve movies from a list of genres, ordered by average rating and then views.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where Genres in ($genres) " +
                            "order by AverageRating as double desc, Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"genres\": [\"Action\", \"Sci-Fi\", \"Thriller\"], \"skip\": 0, \"pageSize\": 15}"
                },
                new AiAgentToolQuery
                {
                    Name = "GetByGenresInListOrderByAverageRatingAsc",
                    Description = "Retrieve movies from a list of genres, ordered by average rating ascending and then views ascending.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where Genres in ($genres) " +
                            "order by AverageRating as double asc, Views as long asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"genres\": [\"Action\", \"Sci-Fi\", \"Thriller\"], \"skip\": 0, \"pageSize\": 15}"
                },
                new AiAgentToolQuery
                {
                    Name = "PopularButUnderrated",
                    Description = "Find popular movies with high view counts but modest ratings, ordered by views descending and rating ascending.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where Views >= $minViews and AverageRating <= $maxRating " +
                            "order by Views as long desc, AverageRating as double asc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"minViews\": 500, \"maxRating\": 3.9, \"skip\": 0, \"pageSize\": 10}"
                },

                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTextGenresRatingDesc",
                    Description = "Search movies by title/tags text or a list of genres, while enforcing quality thresholds (min views & min rating). Results are ordered by average rating descending, then views descending.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where (Views >= $minViews and AverageRating >= $minRating) " +
                            "  and (search(Title, $text) or search(Tags, $text) or Genres in ($genres)) " +
                            "order by AverageRating as double desc, Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\":\"avengers\",\"genres\":[\"Action\",\"Sci-Fi\"],\"minViews\":100,\"minRating\":3.8,\"skip\":0,\"pageSize\":10}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByTextGenresRatingAsc",
                    Description = "Search movies by title/tags text or a list of genres, while enforcing quality thresholds (min views & max rating). Results are ordered by average rating ascending, then views descending.",
                    Query = "from index 'MovieStats/ByGenres/Title/Tags/AverageRating/AndViews' " +
                            "where (Views >= $minViews and AverageRating <= $maxRating) " +
                            "  and (search(Title, $text) or search(Tags, $text) or Genres in ($genres)) " +
                            "order by AverageRating as double asc, Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"text\":\"avengers\",\"genres\":[\"Action\",\"Sci-Fi\"],\"minViews\":100,\"maxRating\":3.8,\"skip\":0,\"pageSize\":10}"
                },
                new AiAgentToolQuery
                {
                    Name = "SearchMoviesByVectorsRatingDesc",
                    Description = "Combine vector similarity on tags/genres with quality thresholds (min views & min rating). Results ordered by average rating desc, then views desc.",
                    Query = "from index 'MovieStats/ByVector/GenresTags/AndAverageRating/AndViews' as i " +
                            "where (i.Views >= $minViews and i.AverageRating >= $minRating) and (vector.search(i.TagsVector, $tagsQuery, 0.95, 500) or vector.search(i.GenresVector, $genresQuery, 0.85, 500)) \r\n" +
                            "order by i.AverageRating as double desc, i.Views as long desc " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"tagsQuery\":\"search for scary movies\",\"genresQuery\":\"search for movies of the genre 'horror'\",\"minViews\":100,\"minRating\":3.8,\"skip\":0,\"pageSize\":10}"
                }
            };

            if (smallDb == false)
            {
                queryTools.Add(new AiAgentToolQuery
                {
                    Name = "GetUserAffinitiesByTags",
                    Description =
                        "Get user tag affinities sorted by score, ordered by average Score (rating) - descending\"",
                    Query = "from index 'UserTagAffinity' " +
                            "where UserId = $userId " +
                            "order by Score desc " +
                            "select Tag, Score, Count " +
                            "limit $skip, $pageSize",
                    ParametersSampleObject = "{\"skip\": 0, \"pageSize\": 10}"
                });
            }

            var actionTools = new List<AiAgentToolAction>()
            {
                new AiAgentToolAction("RateMovie",
                    "Add movie rate for the current user you talking with, required movie name and rate value between 0 to 5 (can be double, doesn't has to be integer)")
                {
                    ParametersSampleObject = JsonConvert.SerializeObject(RateToolSampleRequest.Instance)
                },
                new AiAgentToolAction("AddTags",
                    "Adds one or more user-provided tags to a specified movie. Tags should describe the movie’s characteristics, such as themes, style, or content. Only perform this action if the tags are relevant to the movie; otherwise, do not apply them and inform the user that the tags are not suitable.")
                {
                    ParametersSampleObject = JsonConvert.SerializeObject(AddTagsSampleRequest.Instance)
                },
                new AiAgentToolAction("ChangeUserName",
                    "Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.")
                {
                    ParametersSampleObject = JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                },
            };

            var agent = new AiAgentConfiguration("movie recommender", conn.Name, systemPrompt)
            {
                Identifier = AiAgentIdentifier,
                Queries = queryTools,
                Actions = actionTools
            };

            agent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            agent.ChatTrimming.Tokens.MaxTokensBeforeSummarization = 80_000;
            agent.ChatTrimming.Tokens.MaxTokensAfterSummarization = 5000;
            agent.ChatTrimming.History = new()
            {
                HistoryExpirationInSec = int.MaxValue
            };

            await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance);

            log($"Ai Agent created, with {queryTools.Count} query tools and {actionTools.Count} action tools");

            AiConnectionString CreateAiConnectionString()
            {
                var apiKey = Environment.GetEnvironmentVariable("RAVEN_AI_INTEGRATION_OPENAI_API_KEY");
                return new AiConnectionString
                {
                    Name = "Agent_ConnectionString",
                    Identifier = Guid.NewGuid().ToString(),
                    ModelType = AiModelType.Chat,
                    OpenAiSettings = new OpenAiSettings(apiKey, "https://api.openai.com/", "gpt-5-mini", reasoningEffort: OpenAiReasoningEffort.Minimal, seed: 48)
                };
            }
        }


        // indexes

        private class Ratings_ByMovie_Stats : AbstractIndexCreationTask<Rating, MovieStat>
        {
            public Ratings_ByMovie_Stats()
            {
                Map = ratings => from r in ratings
                    let first = LoadDocument<Movie>(r.MovieId)
                    select new MovieStat
                    {
                        MovieId = r.MovieId,
                        Title = first.Title,
                        Views = 1,
                        RatingsSum = (double)r.RatingValue,
                        AverageRating = 0,
                        Tags = first.Tags,
                        Genres = first.Genres
                    };

                Reduce = results => from r in results
                    group r by r.MovieId
                    into g
                    let totalCount = g.Sum(x => x.Views)
                    let totalSum = g.Sum(x => x.RatingsSum)
                    let first = g.FirstOrDefault()
                    select new MovieStat
                    {
                        MovieId = g.Key,
                        Title = first.Title,
                        Views = totalCount,
                        RatingsSum = totalSum,
                        AverageRating = totalSum / totalCount,
                        Tags = first.Tags,
                        Genres = first.Genres,
                    };

                // quick search by MovieId
                Index(x => x.MovieId, FieldIndexing.Default);

                OutputReduceToCollection = "MovieStats";
            }
        }

        private class Ratings_ByUser_LastRates
            : AbstractIndexCreationTask<Rating, Ratings_ByUser_LastRates.Result>
        {
            public class Result
            {
                public string UserId { get; set; }
                public List<MovieRate> LastRates { get; set; }

                public class MovieRate
                {
                    public string MovieId { get; set; }
                    public string Title { get; set; }
                    public double RateValue { get; set; }
                    public DateTime TimeStamp { get; set; }
                }
            }

            public Ratings_ByUser_LastRates()
            {
                // Map: project each rating with its movie title into a single MovieRate object,
                // grouped initially by UserId (reduce step will combine).
                Map = ratings => from r in ratings
                    let movie = LoadDocument<Movie>(r.MovieId)
                    select new
                    {
                        r.UserId,
                        LastRates = new[]
                        {
                            new Result.MovieRate
                            {
                                MovieId = r.MovieId,
                                Title = movie.Title,
                                RateValue = r.RatingValue,
                                TimeStamp = r.TimeStamp
                            }
                        }
                    };

                // Reduce: group all ratings per UserId, keeping only the latest rating for each MovieId.
                Reduce = results => from r in results
                    group r by r.UserId
                    into g
                    select new
                    {
                        UserId = g.Key,
                        LastRates = g
                            .SelectMany(x => x.LastRates)
                            .GroupBy(x => x.MovieId)
                            .Select(mg =>
                                mg.OrderByDescending(x => x.TimeStamp)
                                    .First()) // getting the last rate in case of more then 1 rate for 1 movie
                            .ToList()
                    };

                // Store all fields so projections can come directly from the index.
                StoreAllFields(FieldStorage.Yes);
                Index(x => x.UserId, FieldIndexing.Exact);
            }
        }

        private class UserTagAffinity : AbstractIndexCreationTask<Rating, UserTagAffinity.Result>
        {
            public class Result
            {
                public string UserId { get; set; }
                public string Tag { get; set; }
                public int Count { get; set; }
                public double Sum { get; set; }
                public double Score { get; set; }
            }

            public UserTagAffinity()
            {
                Map = ratings => from r in ratings
                    let m = LoadDocument<Movie>(r.MovieId)
                    from t in m.Tags
                    select new
                    {
                        r.UserId, Tag = t, Count = 1, Sum = (double)r.RatingValue, Score = (double)r.RatingValue
                    };

                Reduce = results => from r in results
                    group r by new { r.UserId, r.Tag }
                    into g
                    let totalCount = g.Sum(x => x.Count)
                    let totalSum = g.Sum(x => x.Sum)
                    select new
                    {
                        UserId = g.Key.UserId,
                        Tag = g.Key.Tag,
                        Count = totalCount,
                        Sum = totalSum,
                        Score = totalSum / totalCount
                    };

                Index(x => x.UserId, FieldIndexing.Exact);
                Index(x => x.Tag, FieldIndexing.Search);
                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class UserGenreAffinity : AbstractIndexCreationTask<Rating, UserGenreAffinity.Result>
        {
            public class Result
            {
                public string UserId { get; set; }
                public string Genre { get; set; }
                public int Count { get; set; }
                public double Sum { get; set; }
                public double Score { get; set; }
            }

            public UserGenreAffinity()
            {
                Map = ratings => from r in ratings
                    let m = LoadDocument<Movie>(r.MovieId)
                    from gnr in (m.Genres ?? Array.Empty<string>())
                    select new
                    {
                        r.UserId, Genre = gnr, Count = 1, Sum = (double)r.RatingValue, Score = (double)r.RatingValue
                    };

                Reduce = results => from r in results
                    group r by new { r.UserId, r.Genre }
                    into g
                    let totalCount = g.Sum(x => x.Count)
                    let totalSum = g.Sum(x => x.Sum)
                    select new
                    {
                        UserId = g.Key.UserId,
                        Genre = g.Key.Genre,
                        Count = totalCount,
                        Sum = totalSum,
                        Score = totalSum / totalCount
                    };

                Index(x => x.UserId, FieldIndexing.Exact);
                Index(x => x.Genre, FieldIndexing.Search);
                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class MovieStats_ByVector_GenresTags_AndAverageRating_AndViews : AbstractIndexCreationTask<MovieStat,
            MovieStats_ByVector_GenresTags_AndAverageRating_AndViews.Result>
        {
            public MovieStats_ByVector_GenresTags_AndAverageRating_AndViews()
            {
                Map = items => from item in items
                    select new Result
                    {
                        AverageRating = item.AverageRating,
                        Views = item.Views,
                        GenresVector = CreateVector(item.Genres),
                        TagsVector = CreateVector(item.Tags),
                    };

                SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;

                Store(x => x.AverageRating, FieldStorage.Yes);
                Store(x => x.Views, FieldStorage.Yes);
                Vector("GenresVector",
                    factory => factory.SourceEmbedding(VectorEmbeddingType.Text)
                        .DestinationEmbedding(VectorEmbeddingType.Single));
                Vector("TagsVector",
                    factory => factory.SourceEmbedding(VectorEmbeddingType.Text)
                        .DestinationEmbedding(VectorEmbeddingType.Single));
            }

            public class Result
            {
                public double AverageRating { get; set; }
                public double Views { get; set; }
                public object GenresVector { get; set; }
                public object TagsVector { get; set; }
            }
        }

        private class Ratings_ByUser_And_Movie : AbstractIndexCreationTask<Rating>
        {
            public Ratings_ByUser_And_Movie()
            {
                Map = ratings => from r in ratings
                    select new
                    {
                        r.UserId,
                        r.MovieId
                    };

                Indexes.Add(x => x.UserId, FieldIndexing.Exact);
                Indexes.Add(x => x.MovieId, FieldIndexing.Exact);
            }
        }

        private class MovieStats_ByGenres_Title_Tags_AverageRating_AndViews : AbstractIndexCreationTask<MovieStat>
        {
            public MovieStats_ByGenres_Title_Tags_AverageRating_AndViews()
            {
                Map = movieStats => from m in movieStats
                    select new
                    {
                        m.Genres,
                        m.Title,
                        m.Tags,
                        m.AverageRating,
                        m.Views
                    };

                Indexes.Add(x => x.Genres, FieldIndexing.Default);
                Indexes.Add(x => x.AverageRating, FieldIndexing.Default);
                Indexes.Add(x => x.Views, FieldIndexing.Default);
                Indexes.Add(x => x.Title, FieldIndexing.Search);
                Indexes.Add(x => x.Tags, FieldIndexing.Search);
            }
        }

        // dto's

        private class TagCsvRow
        {
            public int userId { get; set; }
            public int movieId { get; set; }
            public string tag { get; set; }
            public long timestamp { get; set; }

            public override string ToString()
            {
                return $"{userId}, {movieId}, {tag}, {timestamp}";
            }
        }

        private class RatingCsvRow
        {
            public int userId { get; set; }
            public int movieId { get; set; }
            public double rating { get; set; }
            public long timestamp { get; set; }

            public Rating ToRating()
            {
                return new Rating
                {
                    Id = "Ratings/",
                    UserId = "Users/" + userId,
                    MovieId = "Movies/" + movieId,
                    RatingValue = rating,
                    TimeStamp = DateTimeOffset.FromUnixTimeSeconds(timestamp).UtcDateTime
                };
            }

            public override string ToString()
            {
                return $"{userId}, {movieId}, {rating}, {timestamp}";
            }
        }

        private class MovieCsvRow
        {
            public int movieId { get; set; }
            public string title { get; set; }
            public string genres { get; set; }

            public Movie ToMovie()
            {
                var match = Regex.Match(this.title, @"^(.*) \((\d{4})\)$");
                if (match.Success == false)
                    return null;

                var title = match.Groups[1].Value.Trim();
                var yearStr = match.Groups[2].Value;

                if (int.TryParse(yearStr, out int year) == false)
                    return null;

                var genresArr = genres.Split('|', StringSplitOptions.RemoveEmptyEntries);

                return new Movie
                {
                    Id = $"Movies/{movieId}",
                    Title = title,
                    Year = year,
                    Genres = genresArr
                };
            }

            public override string ToString()
            {
                return $"{movieId}, {title}, {genres}";
            }
        }


        private static string[] israeliFullNames = new[]
        {
            "Noa Cohen", "Daniel Levi", "Yael Mizrahi", "Avi Biton", "Maya Peretz",
            "Yonatan Azulay", "Tamar Ben-David", "Itay Shalom", "Shira Hadad", "Elior Malka",
            "Noya Avraham", "Omer Baruch", "Roni Golan", "Idan Dahan", "Galit Sabag",
            "Nadav Moyal", "Hila Amram", "Doron Peleg", "Lior Edri", "Einav Saban",
            "Shaked Nahum", "Alon Gabay", "Inbar Sharabi", "Yossi Turgeman", "Meital Even-Chen",
            "Tal Avital", "Eliran Amar", "Orly Alfasi", "Shani Hazan", "Tzachi Azulay",
            "Rotem Regev", "Mor Cohen", "Yarden Ben-Hamo", "Shlomi Chaim", "Karin Shalev",
            "Omri Zakai", "Michal Atias", "Oren Vaknin", "Hodaya Levi", "Bar Shemesh",
            "Ofir Sasson", "Naama Mor", "Ravit Ezra", "Tom Avital", "Yehuda Sharvit",
            "Sivan Goldstein", "Amit Morad", "Lilach Arviv", "Erez Rahamim", "Yaara Menashe",
            "Ziv Ben-Zion", "Or Gavriel", "Tzlil Hadad", "Kfir Maman", "Lihi Bar-On",
            "Ron Levi", "Adi Halimi", "Dvir Cohen", "Ayelet Sharon", "Boaz Nir",
            "Shiran Tal", "Dror Yosef", "Dana Harari", "Assaf Shitrit", "Yarden Romano",
            "Reut Oren", "Yigal Sabag", "Ilana Ravid", "Shachar Avraham", "Maayan Ezra",
            "Tal Bashari", "Netanel Meiri", "Shay Natan", "Liron Menachem", "Raz Shoham",
            "Adi Azulay", "Zohar Ben-Ami", "Yossi Shalom", "Mor Elbaz", "Noy Vaknin",
            "Tamir Gabbay", "Yael Sror", "Elad Mashiach", "Sapir Medina", "Amit Danino",
            "Eden Shitrit", "Bar Refael", "Eli Barak", "Ilan Azulay", "Noga Tamir",
            "Saar Sela", "Ofek Levi", "Liat Rimon", "Yair David", "Yarden Ashkenazi",
            "Gili Orbach", "Oshri Nahmani", "Tzlil Gabay", "Orel Avitan", "Shlomi Ben-Yosef"
        };
    }
}
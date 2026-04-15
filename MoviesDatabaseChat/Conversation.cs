using System.Text.Json;
using MoviesDatabaseChat.Entities;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;

namespace MoviesDatabaseChat
{
    internal class Conversation
    {
        private readonly IDocumentStore _store;
        private readonly string _userId;
        private readonly IAiConversationOperations _chat;

        public Conversation(IDocumentStore store, string chatId, string userId)
        {
            _store = store;
            _userId = userId;
            _chat = store.AI.Conversation(DatabaseBootstrapper.AiAgentIdentifier, chatId,
                creationOptions: new AiConversationCreationOptions().AddParameter("userId", userId));

            _chat.Handle<RateToolSampleRequest>("RateMovie", (r) => RateMovieAsync(store, userId, r));
            _chat.Handle<AddTagsSampleRequest>("AddTags", (r) => AddTagsAsync(store, r));
            _chat.Handle<ChangeUserNameSampleRequest>("ChangeUserName", (r) => ChangeUserNameAsync(store, userId, r));
        }

        public async Task<MoviesSampleObject> TalkAsync(string prompt)
        {
            _chat.SetUserPrompt(prompt);
            var agentResult = await _chat.RunAsync<MoviesSampleObject>(CancellationToken.None);
            return agentResult.Answer;
        }

        public async Task<MoviesSampleObject> TalkWithImagesAsync(string prompt, string[] imagePaths)
        {
            var streams = new List<FileStream>();
            try
            {
                foreach (var imagePath in imagePaths)
                {
                    var stream = File.OpenRead(imagePath);
                    streams.Add(stream);
                    _chat.AddAttachment(Path.GetFileName(imagePath), stream, GetMimeType(imagePath));
                }

                if (!string.IsNullOrEmpty(prompt))
                    _chat.SetUserPrompt(prompt);

                var agentResult = await _chat.RunAsync<MoviesSampleObject>(CancellationToken.None);
                return agentResult.Answer;
            }
            finally
            {
                foreach (var stream in streams)
                    await stream.DisposeAsync();
            }
        }

        private static string GetMimeType(string filePath) =>
            Path.GetExtension(filePath).ToLowerInvariant() switch
            {
                ".jpg" or ".jpeg" => "image/jpeg",
                ".png"            => "image/png",
                ".gif"            => "image/gif",
                ".webp"           => "image/webp",
                _                 => "image/jpeg"
            };

        private static async Task<object> RateMovieAsync(IDocumentStore store, string userId, RateToolSampleRequest req)
        {
            if (req.RateValue < 0 || req.RateValue > 5)
            {
                return new ActionToolResult
                {
                    IsSuccessful = false,
                    Answer = $"Cant rate \"{req.MovieName}\" with the rate value {req.RateValue} - rate value has to be between 0 to 5"
                };
            }

            using (var session = store.OpenAsyncSession())
            {
                var movies = await session
                    .Advanced
                    .AsyncRawQuery<Movie>("from Movies where Title = $name")
                    .AddParameter("name", req.MovieName.ToLower())
                    .ToListAsync();

                if (movies == null || movies.Count == 0)
                {
                    return new ActionToolResult
                    {
                        IsSuccessful = false,
                        Answer = $"Movie with the name \"{req.MovieName}\" doesn't exist on the database"
                    };
                }

                var user = await session.LoadAsync<User>(userId);

                foreach (var m in movies)
                {
                    user.WatchedMovies.Add(m.Id);
                    await session.StoreAsync(new Rating()
                    {
                        Id = "Ratings/",
                        MovieId = m.Id,
                        UserId = userId,
                        RatingValue = req.RateValue,
                        TimeStamp = DateTime.Now
                    });
                }

                await session.SaveChangesAsync();

                return new ActionToolResult
                {
                    IsSuccessful = true,
                    Answer =
                        $"Found {movies.Count} movies with the name '{req.MovieName}' and rated them by score '{req.RateValue}'"
                };
            }
        }

        private static async Task<object> AddTagsAsync(IDocumentStore store, AddTagsSampleRequest req)
        {
            using (var session = store.OpenAsyncSession())
            {
                var movies = await session
                    .Advanced
                    .AsyncRawQuery<Movie>("from Movies where Title = $name")
                    .AddParameter("name", req.MovieName.ToLower())
                    .ToListAsync();

                if (movies == null || movies.Count == 0)
                {
                    return new ActionToolResult
                    {
                        IsSuccessful = false,
                        Answer =
                            $"Movie with the name \"{req.MovieName}\" doesn't exist on the database"
                    };
                }

                foreach (var m in movies)
                {
                    foreach (var t in req.Tags)
                    {
                        m.Tags.Add(t);
                    }
                }

                await session.SaveChangesAsync();

                return new ActionToolResult 
                {
                    IsSuccessful = true,
                    Answer =
                        $"Found {movies.Count} movies with the name '{req.MovieName}' and added them by tags [{string.Join(", ", req.Tags)}]"
                };
            }
        }

        private static async Task<object> ChangeUserNameAsync(IDocumentStore store, string userId, ChangeUserNameSampleRequest req)
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(userId);
                if (user.Name.ToLower() != req.OldUserName.ToLower())
                {
                    return new ActionToolResult
                    {
                        IsSuccessful = false,
                        Answer = $"Your old name isn't '{req.OldUserName}'"
                    };
                }

                user.Name = req.NewUserName;
                await session.SaveChangesAsync();

                return new ActionToolResult
                {
                    IsSuccessful = true,
                    Answer = $"Name of user '{user.Id}' changed from '{req.OldUserName}' to '{req.NewUserName}'"
                };
            }
        }
    }
}

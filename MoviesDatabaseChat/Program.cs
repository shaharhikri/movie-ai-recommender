using MoviesDatabaseChat;
using MoviesDatabaseChat.Entities;
using Raven.Client.Documents;
using System.Linq;

class Program
{
    public static async Task Main()
    {
        using var store = new DocumentStore
        {
            Urls = new[] { "http://localhost:8080" },
            Database = "MoviesDB2"
        }.Initialize();

        if (await DatabaseBootstrapper.CreateDatabaseAsync(store, log: Console.WriteLine, smallDb: true))
        {
            Console.WriteLine($"Database '{store.Database}' is ready on your local server, run again for chat");
            return;
        }

        // db is already exists -> start conversation
        // /image "C:\Users\Shahar Hikri\Desktop\images.jpg" "C:\Users\Shahar Hikri\Desktop\images2.jpg" recommend a movie based on the movie on the image
        // /image "C:\Users\Shahar Hikri\Desktop\images.jpg" "C:\Users\Shahar Hikri\Desktop\images2.jpg" What do you see in these images? Can you recognize if they are movie covers?
        // /image "C:\Users\Shahar Hikri\Desktop\images.jpg" what is the movie on the photo
        await StartConversationAsync(store);
    }

    private static async Task StartConversationAsync(IDocumentStore store)
    {
        var (chatId, userId) = GetChatIdAndUserId();

        var conversation = new Conversation(store, chatId, userId);

        Console.ForegroundColor = ConsoleColor.DarkCyan;
        Console.WriteLine("Conversation started (write something to the agent): ");
        Console.ForegroundColor = ConsoleColor.DarkGray;
        Console.WriteLine("  Tip: use  /image <path> [path2 ...] [question]  to send one or more movie covers or photos.");
        Console.ForegroundColor = ConsoleColor.White;
        var consoleReader = new HistoryConsoleReader();

        while (true)
        {
            var input = consoleReader.ReadLine().TrimStart(); // same as 'Console.ReadLine()' but with history - can use arrow up/down keys to get previous inputs

            if (string.IsNullOrEmpty(input))
            {
                PrintEmptyAnswer();
                continue;
            }

            if (await ShouldEndChatAsync(store, chatId, input))
                break;

            if (input.StartsWith("/image ", StringComparison.OrdinalIgnoreCase))
            {
                await HandleImageInputAsync(conversation, input);
                continue;
            }

            var answer = await conversation.TalkAsync(input);
            PrintAnswer(answer);
        }

        Console.WriteLine("Goodbye!");
    }

    private static (string chatId, string userId) GetChatIdAndUserId()
    {
        Console.ForegroundColor = ConsoleColor.DarkCyan;
        Console.Write("Enter ChatId: ");
        Console.ForegroundColor = ConsoleColor.White;
        string chatId = Console.ReadLine().Trim();
        if (chatId == string.Empty)
        {
            chatId = "Chats/";
            Console.SetCursorPosition(14, Console.CursorTop - 1);
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine(chatId);
            Console.ForegroundColor = ConsoleColor.White;
        }
        Console.ForegroundColor = ConsoleColor.DarkCyan;
        Console.Write("Enter UserId: ");
        Console.ForegroundColor = ConsoleColor.White;
        string userId = Console.ReadLine().Trim();
        if (userId == string.Empty)
        {
            userId = "Users/1";
            Console.SetCursorPosition(14, Console.CursorTop - 1);
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine(userId);
            Console.ForegroundColor = ConsoleColor.White;
        }

        return (chatId, userId);
    }

    private static async Task<bool> ShouldEndChatAsync(IDocumentStore store, string chatId, string input)
    {
        if (input?.ToLower().Trim() == "exit")
            return true;

        if (input?.ToLower().Trim() == "exit and remove chat")
        {
            using var session = store.OpenAsyncSession();
            session.Delete(chatId);
            await session.SaveChangesAsync();
            return true;
        }

        return false;
    }

    private static void PrintAnswer(MoviesSampleObject movie)
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"Answer: {movie.Answer}");
        Console.ForegroundColor = ConsoleColor.DarkGray;
        if (movie.MoviesIds.Count > 0)
            Console.WriteLine($"Movies ids: [{string.Join(", ", movie.MoviesIds)}]");

        if (movie.MoviesNames.Count > 0)
            Console.WriteLine($"Movies names: [{string.Join(", ", movie.MoviesNames)}]");
        Console.ForegroundColor = ConsoleColor.White;
    }

    private static void PrintEmptyAnswer()
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.SetCursorPosition(0, Console.CursorTop - 1);
        Console.WriteLine($"Prompt cannot be empty, try again");
        Console.ForegroundColor = ConsoleColor.White;
    }

    private static async Task HandleImageInputAsync(Conversation conversation, string input)
    {
        var (imagePaths, prompt) = ParseImageCommand(input);

        foreach (var imagePath in imagePaths)
        {
            if (!File.Exists(imagePath))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Image file not found: {imagePath}");
                Console.WriteLine("Usage: /image <path> [path2 ...] [question]");
                Console.WriteLine("       /image \"cover1.jpg\" \"cover2.png\" what movies are these?");
                Console.ForegroundColor = ConsoleColor.White;
                return;
            }

            string ext = Path.GetExtension(imagePath).ToLowerInvariant();
            if (ext is not (".jpg" or ".jpeg" or ".png" or ".gif" or ".webp"))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Unsupported image format '{ext}'. Use: jpg, jpeg, png, gif, webp");
                Console.ForegroundColor = ConsoleColor.White;
                return;
            }
        }

        if (string.IsNullOrEmpty(prompt))
            prompt = "What can you tell me about these images? Are they movie covers? If so, what movies are they and what can you recommend based on them?";

        Console.ForegroundColor = ConsoleColor.DarkYellow;
        Console.WriteLine($"Sending {imagePaths.Length} image(s): {string.Join(", ", imagePaths.Select(Path.GetFileName))}");
        Console.ForegroundColor = ConsoleColor.White;

        var answer = await conversation.TalkWithImagesAsync(prompt, imagePaths);
        PrintAnswer(answer);
    }

    // Parses: /image "path1" "path2" optional prompt
    //     or: /image path_no_spaces optional prompt
    // Returns (imagePaths, prompt)
    private static (string[] imagePaths, string prompt) ParseImageCommand(string input)
    {
        string rest = input.Substring("/image ".Length).Trim();
        var paths = new List<string>();

        // Collect all leading quoted paths
        while (rest.StartsWith("\""))
        {
            int closeQuote = rest.IndexOf('"', 1);
            if (closeQuote < 0)
            {
                paths.Add(rest.TrimStart('"'));
                return (paths.ToArray(), string.Empty);
            }

            paths.Add(rest.Substring(1, closeQuote - 1));
            rest = rest.Substring(closeQuote + 1).Trim();
        }

        if (paths.Count > 0)
            return (paths.ToArray(), rest);

        // No quotes: try substrings from longest to shortest to find a single file path
        string[] parts = rest.Split(' ');
        for (int i = parts.Length; i >= 1; i--)
        {
            string candidate = string.Join(" ", parts[..i]);
            if (File.Exists(candidate))
            {
                string promptPart = string.Join(" ", parts[i..]).Trim();
                return (new[] { candidate }, promptPart);
            }
        }

        // Not found — return as-is so caller can show a clear error
        return (new[] { rest }, string.Empty);
    }

}
class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== 채팅 서버 ===");
        Console.Write("포트 번호 입력 (기본: 8080): ");
        string portInput = Console.ReadLine();
        int port = string.IsNullOrEmpty(portInput) ? 8080 : int.Parse(portInput);
        
        var server = new ChatServer(port);
        await server.StartAsync();
    }
}
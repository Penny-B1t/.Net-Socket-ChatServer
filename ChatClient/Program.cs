class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== 채팅 클라이언트 ===");
        
        Console.Write("서버 IP (기본: 127.0.0.1): ");
        string ip = Console.ReadLine();
        if (string.IsNullOrEmpty(ip)) ip = "127.0.0.1";
        
        Console.Write("포트 (기본: 8080): ");
        string portInput = Console.ReadLine();
        int port = string.IsNullOrEmpty(portInput) ? 8080 : int.Parse(portInput);
        
        Console.Write("사용자 이름: ");
        string name = Console.ReadLine();
        if (string.IsNullOrEmpty(name)) name = $"User_{new Random().Next(1000, 9999)}";
        
        var client = new ChatClient();
        await client.StartAsync(ip, port, name);
    }
}
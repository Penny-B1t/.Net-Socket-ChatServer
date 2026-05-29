using System.Net.Sockets;
using System.Text;
using System.Buffers;


public class ChatClient
{
    private Socket _socket;
    private string _name;
    private bool _isConnected;
    private byte[] _buffer;
    private readonly ArrayPool<byte> _bufferPool = ArrayPool<byte>.Shared;
    private readonly MessageProtocol.MessageDecoder _decoder = new MessageProtocol.MessageDecoder();
    
    public async Task StartAsync(string serverIp, int port, string userName)
    {
        _name = userName;
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _buffer = _bufferPool.Rent(4096);
        
        try
        {
            Console.WriteLine($"서버 {serverIp}:{port} 연결 중...");
            await _socket.ConnectAsync(serverIp, port);
            _isConnected = true;
            Console.WriteLine("연결 성공!\n");
            
            // 서버에 이름 전송 (처음 한 번만)
            byte[] nameData = Encoding.UTF8.GetBytes(_name);
            await _socket.SendAsync(nameData, SocketFlags.None);
            
            // 수신 Task와 송신 Task 병렬 실행
            var receiveTask = ReceiveMessagesAsync();
            var sendTask = SendMessagesAsync();
            
            await Task.WhenAny(receiveTask, sendTask);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"오류: {ex.Message}");
        }
        finally
        {
            Disconnect();
        }
    }
    
    private async Task ReceiveMessagesAsync()
    {
        try
        {
            while (_isConnected && _socket.Connected)
            {
                int bytesRead = await _socket.ReceiveAsync(_buffer, SocketFlags.None);
                if (bytesRead == 0) break;
                
                var messages = _decoder.Parse(_buffer.AsSpan(0, bytesRead));
                foreach (string json in messages)
                {
                    var message = ChatMessage.FromJson(json);
                    DisplayMessage(message);
                }
            }
        }
        catch (SocketException)
        {
            Console.WriteLine("서버 연결 끊김");
        }
    }
    
    private async Task SendMessagesAsync()
    {
        while (_isConnected)
        {
            string input = await Task.Run(() => Console.ReadLine());
            
            if (string.IsNullOrEmpty(input)) continue;
            
            // 명령어 처리
            if (input.StartsWith("/"))
            {
                await ProcessCommandAsync(input);
            }
            else if (input.StartsWith("@"))
            {
                // 귓속말 형식: @사용자명 메시지
                int spaceIndex = input.IndexOf(' ');
                if (spaceIndex > 0)
                {
                    string target = input.Substring(1, spaceIndex - 1);
                    string content = input.Substring(spaceIndex + 1);
                    
                    await SendMessageAsync(new ChatMessage
                    {
                        Type = MessageType.Whisper,
                        Target = target,
                        Content = content
                    });
                }
            }
            else
            {
                // 일반 채팅
                await SendMessageAsync(new ChatMessage
                {
                    Type = MessageType.Chat,
                    Content = input
                });
            }
        }
    }
    
    private async Task ProcessCommandAsync(string command)
    {
        switch (command.ToLower())
        {
            case "/list":
                await SendMessageAsync(new ChatMessage
                {
                    Type = MessageType.ClientList
                });
                break;
                
            case "/exit":
                Console.WriteLine("종료 중...");
                _isConnected = false;
                break;
                
            default:
                Console.WriteLine("명령어: /list (접속자 목록), /exit (종료)");
                break;
        }
    }
    
    private async Task SendMessageAsync(ChatMessage message)
    {
        if (!_isConnected) return;
        
        string json = message.ToJson();
        byte[] data = MessageProtocol.Encode(json);
        await _socket.SendAsync(data, SocketFlags.None);
    }
    
    private void DisplayMessage(ChatMessage message)
    {
        string color = Console.ForegroundColor.ToString();
        string time = message.Timestamp.ToString("HH:mm:ss");
        
        switch (message.Type)
        {
            case MessageType.Chat:
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine($"[{time}] {message.Sender}: {message.Content}");
                break;
                
            case MessageType.Whisper:
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"[{time}] (귓속말) {message.Sender}: {message.Content}");
                break;
                
            case MessageType.Join:
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"[{time}] ★ {message.Content} ★");
                break;
                
            case MessageType.Leave:
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"[{time}] ★ {message.Content} ★");
                break;
                
            case MessageType.ClientList:
                Console.ForegroundColor = ConsoleColor.Magenta;
                Console.WriteLine($"[{time}] 접속자 목록: {message.Content}");
                break;
                
            case MessageType.Error:
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[{time}] 오류: {message.Content}");
                break;
        }
        
        Console.ResetColor();
    }
    
    private void Disconnect()
    {
        _isConnected = false;
        _socket?.Close();
        if (_buffer != null)
        {
            _bufferPool.Return(_buffer);
            _buffer = null;
        }
        _decoder.Clear();
        Console.WriteLine("연결 종료");
    }
}
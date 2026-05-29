using System.Net.Sockets;
using System.Text;
using System.Buffers;

public class ClientSession
{
    private static int _nextId = 0;
    private readonly Socket _socket;
    private readonly ChatServer _server; 
    private readonly ArrayPool<byte> _bufferPool;
    private byte[]? _buffer;
    private const int BUFFER_SIZE = 4096; // 최대 사용가능 버퍼 크기

    private bool _isConnected;
    private readonly MessageProtocol.MessageDecoder _decoder;

    public int Id { get; }
    public string ClientName { get; set; }

    // 생성자 함수
    public ClientSession(Socket socket, ChatServer server)
    {
        Id = Interlocked.Increment(ref _nextId); // 원자적으로 값 증가
        _socket = socket;
        _server = server;
        _bufferPool = server.GetBufferPool();
        _buffer = _bufferPool.Rent(BUFFER_SIZE); // 연결 1개당 버퍼 1개 사용
        _isConnected = true;
        _decoder = new MessageProtocol.MessageDecoder();
    }

    // 세션 내부 데이터 송신 스레드
    public async Task StartAsync()
    {
        try 
        {

            // 세션 생성 초기 사용자 이름 수신
            ClientName = await ReceiveClientNameAsync();
            // 세션 내 등록
            await _server.RegisterClientAsync(this, ClientName);

            // 메시지 수신 루프 
            while(_isConnected && _socket.Connected)
            {
                int byteRead = await _socket.ReceiveAsync(_buffer!, SocketFlags.None);

                if(byteRead == 0)
                {
                    break;
                }

                var messages = _decoder.Parse(_buffer!.AsSpan(0, byteRead));
                
                foreach (string jsonMessage in messages)
                {
                    await ProcessMessageAsync(jsonMessage); // 순서 보장하며 안전하게 비동기 순차 처리
                }
                
            }

        }
        catch(SocketException)
        {
            // 클라이언트 연결 종료
            Console.WriteLine($"[소켓 오류] {Id} 연결 끊김");
        }
        finally
        {
            Disconnect();
        }
    }

    private async Task<string> ReceiveClientNameAsync()
    {
        // 1초 타임아웃으로 이름 수신
        using var cts = new CancellationTokenSource(1000);
        byte[]? buffer = _buffer;
        if (buffer == null) return $"User_{Id}";
        
        try
        {
            int bytesRead = await _socket.ReceiveAsync(buffer, SocketFlags.None, cts.Token);
            if (bytesRead > 0)
            {
                return Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("이름 수신 타임아웃, 기본 이름 사용");
        }
        
        return $"User_{Id}";  // 기본 이름
    }

    // 메시
    public async Task ProcessMessageAsync(string jsonMessage)
    {
        try
        {
            var message = ChatMessage.FromJson(jsonMessage);
            message.Sender = ClientName;
            message.Timestamp = DateTime.Now;

            switch (message.Type)
            {
                case MessageType.Chat:
                    // 경우에 따라 사용자가 보낸 메시지는 다시 자신에게 안보내주는 기능 추가하기
                    await _server.BroadcastAsync(message);
                    Console.WriteLine($"[채팅] {ClientName}: {message.Content}");
                    break;
                case MessageType.Whisper:
                    if (!string.IsNullOrEmpty(message.Target)){
                        await _server.WhisperAsync(message.Target, message);
                        Console.WriteLine($"[귓속말] {ClientName} -> {message.Target}: {message.Content}");
                    }
                    break;
                case MessageType.ClientList:
                    await _server.SendClientListAsync(this);
                    break;
                default:
                    break;
            }
        }
        catch (Exception ex)
        {
             Console.WriteLine($"메시지 처리 오류: {ex.Message}");
        }
    }

    // 함수 오버로드를 사용하여 두가지 타입의 데이터 처리
    public async Task SendAsync(string message)
    {
        if (!_isConnected || !_socket.Connected) return;

        byte[] data = MessageProtocol.Encode(message);
        await _socket.SendAsync(data, SocketFlags.None);
    }
    public async Task SendAsync(byte[] data)
    {
        if (!_isConnected || !_socket.Connected) return;

        await _socket.SendAsync(data, SocketFlags.None);
    }

    public void Disconnect()
    {
        if (!_isConnected) return;

        _isConnected = false;
        try
        {
            // 연결된 프로세스 갯수 상관없이 연결 종료
            _socket.Shutdown(SocketShutdown.Both);
            _socket.Close();
        }
        catch (Exception)
        {
            // 소켓 관련 오류 무시 : 이미 연결이 끊어졌기 때문에 발생하는 오류
            // 정상적인 상황에서 오류가 발생하면 소켓을 종료할 수 없다는 것은 이미 연결이 끊어졌다는 뜻 
            
        }

        if (_buffer != null)
        {
            _bufferPool.Return(_buffer);
            _buffer = null;
        }

        _decoder.Clear();

        _ = _server.UnregisterClientAsync(this); // 세션 등록 해제
    }

}
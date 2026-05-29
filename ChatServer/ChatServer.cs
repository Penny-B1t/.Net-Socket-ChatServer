using System.Collections.Concurrent;
using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;


public class ChatServer
{

    private readonly Socket _listener; // 서버 객체
    private readonly ConcurrentDictionary<string, ClientSession> _clients; // 세션 매니저
    private bool _isRunning;

    // 버퍼 pool
    private readonly ArrayPool<byte> _bufferPool;
    
    public ChatServer(int port)
    {

        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listener.Bind(new IPEndPoint(IPAddress.Any, port));
        _clients = new ConcurrentDictionary<string, ClientSession>();
        _bufferPool = ArrayPool<byte>.Shared; // 공유 풀 선언
    }

    public async Task StartAsync()
    {
        _listener.Listen(100); // 최대 100개의 연결 대기
        _isRunning = true;

        Console.WriteLine($"채팅 서버 시작됨 (포트: {((IPEndPoint)_listener.LocalEndPoint!).Port})");
        Console.WriteLine("명령어: /list (접속자 목록), /exit (서버 종료)\n");
        
        // 메인 루프와 콘솔 입력 병렬 실행 
        var acceptTask = AcceptClientsAsync();
        var consoleTask = HandleConsoleAsync();
        
        // Task.WhenAny : 둘 중 하나만 끝나면 리턴. 
        await Task.WhenAny(acceptTask, consoleTask);
        await StopAsync();
    }

    public async Task AcceptClientsAsync()
    {
        while(_isRunning)
        {
            try
            {
                var clientSocket = await _listener.AcceptAsync();
                Console.WriteLine($"클라이언트 접속: {clientSocket.RemoteEndPoint}");
                
                // 세션 생성
                var session = new ClientSession(clientSocket, this);
                _ = session.StartAsync(); // 사용자별 핸들러 실행

            }
            catch(ObjectDisposedException)
            {
                // ObjectDisposedException 오류는 서버가 정상적으로 종료되었을 때 발생하므로, 무시
                break; // 서버 종료 시 루프 탈출 
            }
            catch (Exception ex)
            {
                Console.WriteLine($"클라이언트 수락 오류: {ex.Message}");
            }
        }
    }

    public async Task RegisterClientAsync(ClientSession session, string requestedName)
    {
        string clientName = requestedName;
        int suffix = 1;

        // id 중복 확인 및 처리
        while(_clients.ContainsKey(clientName))
        {
            clientName = $"{requestedName}_{suffix}";
            suffix++;
        }

        // 등록 처리
        session.ClientName = clientName;
        _clients[clientName] = session;

        await BroadcastAsync(new ChatMessage
        {
            Type = MessageType.Join,
            Sender = "System",
            Content = $"{clientName}님이 접속했습니다.",
            Timestamp = DateTime.Now,
        });

        Console.WriteLine($"접속자: {clientName} (총 {_clients.Count}명)");
        
    }   

    public async Task UnregisterClientAsync(ClientSession session)
    {
        if(_clients.TryRemove(session.ClientName, out _))
        {
            // 퇴장 알림
            await BroadcastAsync(new ChatMessage{
                Type = MessageType.Leave,
                Sender = "System",
                Content = $"{session.ClientName}님이 퇴장했습니다.",
                Timestamp = DateTime.Now,
            });

            Console.WriteLine($"퇴장: {session.ClientName} (남은 {_clients.Count}명)");
        }
    }

    // 세션에 존재하는 모든 사용자의 socket을 사용하여 메시지를 전송
    // excludeClient : 메시지를 제외할 사용자의 이름 (null = 모두에게 전송)
    public async Task BroadcastAsync(ChatMessage message, string excludeClient = null)
    {
        string json = message.ToJson();
        byte[] data = MessageProtocol.Encode(json);

        var tasks = _clients.Values
        .Where(c => excludeClient == null || c.ClientName != excludeClient)
        .Select(c => c.SendAsync(data));

        // non-blocking 동시 전송 (동작 보장)
        // 여러 Task가 하나라도 실패하면 전체 Task.WhenAll이 실패
        await Task.WhenAll(tasks);
    }

    public async Task WhisperAsync(string targetName, ChatMessage message)
    {
        // 타켓이 존재하는지 확인
        if(_clients.TryGetValue(targetName, out var targetSession))
        {
            message.Type = MessageType.Whisper;
            await targetSession.SendAsync(message.ToJson());

            if(_clients.TryGetValue(message.Sender, out var senderSession))
            {
                var confirmMsg = new ChatMessage
                {
                    Type = MessageType.Whisper,
                    Sender = "System",
                    Content = $"[귓속말 전송됨 -> {targetName}]: {message.Content}",
                    Timestamp = DateTime.Now
                };

                await senderSession.SendAsync(confirmMsg.ToJson());
            }
            
        } else {
            if(_clients.TryGetValue(message.Sender, out var senderSession))
            {
                var errorMsg = new ChatMessage
                {
                    Type = MessageType.Whisper,
                    Sender = "System",
                    Content = $"'{targetName}' 님이 존재하지 않습니다.",
                    Timestamp = DateTime.Now
                };

                await senderSession.SendAsync(errorMsg.ToJson());
            }
        }
    }

    public async Task SendClientListAsync(ClientSession session)
    {
        // 현재 접속중인 사용자의 이름만 추출
        var clientNames = _clients.Keys.ToList();

        var clientListMsg = new ChatMessage
        {
            Type = MessageType.ClientList,
            Sender = "System",
            Content = string.Join(", ", clientNames),
            Timestamp = DateTime.Now
        };

        await session.SendAsync(clientListMsg.ToJson());
    }

    public async Task HandleConsoleAsync()
    {
        await Task.Run(() => 
        {
            while(_isRunning)
            {
                var input = Console.ReadLine();
                if (input == "/list")
                {
                    Console.WriteLine($"\n=== 현재 접속자: {_clients.Count}명 ===");
                    foreach (var name in _clients.Keys)
                    {
                        Console.WriteLine($"  - {name}");
                    }
                    Console.WriteLine();
                }
                else if (input == "/exit")
                {
                    _isRunning = false;
                    break;
                }
                else if (input == "/help")
                {
                    Console.WriteLine("\n === 사용가능 명령어 ===");
                    Console.WriteLine(" /list   : 접속자 목록 보기");
                    Console.WriteLine(" /exit   : 서버 종료");
                    Console.WriteLine();
                }
                
            }
        });
    }

    public async Task StopAsync()
    {
        Console.WriteLine("서버 종료 중");

        _isRunning = false;

        await BroadcastAsync(new ChatMessage
        {
            Type = MessageType.Leave,
            Sender = "System",
            Content = "서버가 종료됩니다.",
            Timestamp = DateTime.Now
        });

        foreach (var client in _clients.Values)
        {
            client.Disconnect();
        }

        _listener.Close();

    }

    // 버퍼풀 반환
    public ArrayPool<byte> GetBufferPool() => _bufferPool;
    // 현재 접속자 수 반환
    public int GetClientCount() => _clients.Count;

}
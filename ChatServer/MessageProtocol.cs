using System.Buffers;
using System.Buffers.Binary;
using System.Text;

public static class MessageProtocol
{
    // 메시지 길이
    public const int HeaderSize = sizeof(int);
    public const int MaxMessageSize = 1024 * 1024; // 최대 Payload 사이즈 제한

    public static byte[] Encode(string message)
    {
        byte[] messageData = Encoding.UTF8.GetBytes(message);

        if(messageData.Length > MaxMessageSize)
        {
            throw new ArgumentException($"Message too large {messageData.Length}");
        }

        byte[] packet = new byte[HeaderSize + messageData.Length];
        // big-endian : 네트워크 표준 엔디언
        BinaryPrimitives.WriteInt32BigEndian(packet, messageData.Length);
        // 페이로드 복사
        Buffer.BlockCopy(messageData, 0, packet, HeaderSize, messageData.Length);

        return packet;
    }

    // 현재 코드 실행은 동기적 실행으로 손실이 발생한다.
    // 또한 내부 buffer 작업으로 new Byte[]할당되면서 GC에 부담을 주는 문제도 동시 발생
    // 지연 실행 시 내부적으로 class와 같은 부가적 선언으로 추가 부하가 발생함으로 Span과 foreach를 사용한 최적화 진행

    public class MessageDecoder
    {
        // ArrayPool 대여 사용하며, 미리 buffer를 넉넉하게 선언하지 않는 이유는 
        // 공간 부족 시 확보를 위한 별도 추가 할당 혹은 공간 확보라는 과정으로 GC 부담을 덜어주고자 함
        private readonly ArrayPool<byte> _pool = ArrayPool<byte>.Shared;
        private byte[]? _buffer;
        private int _bufferSize;
        private int _expectedLength = -1; 

        public MessageDecoder()
        {
            // 상위 네트워크 수신 버퍼 크기와 맞춤 (기본 대여)
            _buffer = _pool.Rent(4096);
            _bufferSize = 0;
        }

        public List<string> Parse(ReadOnlySpan<byte> newData)
        {
            var messages = new List<string>();
            byte[]? buffer = _buffer;
            if (buffer == null) return messages;

            if (_bufferSize + newData.Length > buffer.Length)
            {
                byte[] newBuffer = _pool.Rent(buffer.Length * 2);
                Buffer.BlockCopy(buffer, 0, newBuffer, 0, _bufferSize);
                _pool.Return(buffer);
                _buffer = newBuffer;
                buffer = newBuffer;
            }
            
            // 배열의 참조 위치를 변경하고 newData를 메모리에 복사하는 행위
            newData.CopyTo(buffer.AsSpan(_bufferSize));
            // 이후 버퍼 크기를 증가시켜 마지막 작업 위치를 변경합니다. 이로써 수신된 패킷을 분리할 수 있게됩니다. 
            _bufferSize += newData.Length;

            int offset = 0;

            while(true)
            {
                int available = _bufferSize - offset;

                if (_expectedLength == -1)
                {
                    // 해더 크기 4byte보다 최소한 데이터가 들어왔을 경우 
                    if (available >= HeaderSize)
                    {
                        ReadOnlySpan<byte> headerSpan = new ReadOnlySpan<byte>(buffer, offset, HeaderSize);
                        _expectedLength = BinaryPrimitives.ReadInt32BigEndian(headerSpan);
                        offset += HeaderSize;
                        available -= HeaderSize;
                        
                        if (_expectedLength <= 0 || _expectedLength > MaxMessageSize)
                        {
                            _expectedLength = -1;
                            throw new InvalidDataException($"Invalid message length: {_expectedLength}");
                        }
                    }
                    else break; // 헤더조차 부족하므로 대기
                }

                // 데이터(페이로드)가 모두 준비된 경우
                if (_expectedLength > 0)
                {
                    if (available >= _expectedLength)
                    {
                        string message = Encoding.UTF8.GetString(buffer, offset, _expectedLength);
                        messages.Add(message);
                        offset += _expectedLength;
                        _expectedLength = -1; // 다음 메시지 대기 상태로 전환
                    }
                    else break; // 페이로드 데이터가 다 올 때까지 대기
                }
            }

            // 데이터가 남아있다면 앞으로 당겨쓰기 (데이터 유실 방지)
            if (offset > 0)
            {
                if (offset < _bufferSize)
                {
                    Buffer.BlockCopy(buffer, offset, buffer, 0, _bufferSize - offset);
                }
                _bufferSize -= offset;
            }

            return messages;
        }

        public void Clear()
        {
            if (_buffer != null)
            {
                _pool.Return(_buffer);
                _buffer = null;
            }
        }
    }
}
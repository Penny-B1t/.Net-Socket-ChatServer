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

    public class MessageDecoder
    {
        private readonly ArrayPool<byte> _pool = ArrayPool<byte>.Shared;
        private byte[]? _buffer;
        private int _bufferSize;
        private int _expectedLength = -1; 

        public MessageDecoder()
        {
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
            
            newData.CopyTo(buffer.AsSpan(_bufferSize));
            _bufferSize += newData.Length;

            int offset = 0;

            while(true)
            {
                int available = _bufferSize - offset;

                if (_expectedLength == -1)
                {
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
                    else break;
                }

                if (_expectedLength > 0)
                {
                    if (available >= _expectedLength)
                    {
                        string message = Encoding.UTF8.GetString(buffer, offset, _expectedLength);
                        messages.Add(message);
                        offset += _expectedLength;
                        _expectedLength = -1;
                    }
                    else break;
                }
            }

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

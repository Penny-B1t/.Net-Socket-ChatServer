using System.Text.Json;

public enum MessageType
{
    Chat,
    Whisper,
    Join,
    Leave,
    ClientList,
    Error,
}

public class ChatMessage
{

    public MessageType Type { get; set; }
    public string Sender { get; set; }
    public string Target { get; set; }
    public string Content { get; set; }
    public DateTime Timestamp { get; set; } 

    public string ToJson()=> JsonSerializer.Serialize(this);
    public static ChatMessage FromJson(string json)=> JsonSerializer.Deserialize<ChatMessage>(json);

}
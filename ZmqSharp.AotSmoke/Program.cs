using ZmqSharp.Messages;

using var message = ZMessage.FromOwned([1, 2, 3]);
Console.WriteLine(message.Count);

namespace ZmqSharp;

/// <summary>
/// Which receive surface a socket composes by default (0023). <see
/// cref="Queue"/> is the default for every concrete socket that can deliver
/// messages: each peer's messages land in that peer's bounded queue and are
/// read through <see cref="ZQueueSocketBase.Messages"/>. <see
/// cref="Callback"/> opts out: the socket binds no queue and the raw
/// <c>OnFrame</c> surface is the delivery path (a custom
/// <see cref="ZSocketOptions.MessageSink"/> implies callback semantics and
/// makes this option redundant). REQ and REP are excluded either way - their
/// protocol cores consume inbound messages before any surface could see them.
/// </summary>
public enum ZReceiveSurface
{
    Queue,
    Callback
}

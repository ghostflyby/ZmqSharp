namespace ZmqSharp.Patterns;

/// <summary>
/// The built-in socket-type identities (0015 section 2.2). Compatibility is
/// declared per endpoint: each member's <see cref="ZSocketType.AcceptsPeer"/>
/// predicate states which peer Socket-Type this endpoint accepts, preserving
/// the asymmetric libzmq matrix (REQ accepts REP, PUSH accepts PULL, PUB
/// accepts SUB, ...). The eleven standard names interoperate with
/// libzmq/NetMQ; a custom <see cref="ZSocketType"/> (any other name)
/// interoperates only between ZmqSharp endpoints whose peers advertise the
/// same name (0015 section 2.3).
/// </summary>
public static class ZSocketTypes
{
    /// <summary>PAIR: accepts a peer advertising PAIR (one-to-one).</summary>
    public static ZSocketType Pair { get; } = new()
    {
        Name = "PAIR",
        AcceptsPeer = peerType => peerType == "PAIR"
    };

    /// <summary>DEALER: accepts DEALER, REP, or ROUTER peers; advertises an identity (0025).</summary>
    public static ZSocketType Dealer { get; } = new()
    {
        Name = "DEALER",
        AcceptsPeer = peerType => peerType is "DEALER" or "REP" or "ROUTER",
        AdvertisesIdentity = true
    };

    /// <summary>REQ: accepts a peer advertising REP; advertises an identity (0025).</summary>
    public static ZSocketType Req { get; } = new()
    {
        Name = "REQ",
        AcceptsPeer = peerType => peerType == "REP",
        AdvertisesIdentity = true
    };

    /// <summary>REP: accepts REQ or DEALER peers.</summary>
    public static ZSocketType Rep { get; } = new()
    {
        Name = "REP",
        AcceptsPeer = peerType => peerType is "REQ" or "DEALER"
    };

    /// <summary>PUSH: accepts a peer advertising PULL.</summary>
    public static ZSocketType Push { get; } = new()
    {
        Name = "PUSH",
        AcceptsPeer = peerType => peerType == "PULL"
    };

    /// <summary>PULL: accepts a peer advertising PUSH.</summary>
    public static ZSocketType Pull { get; } = new()
    {
        Name = "PULL",
        AcceptsPeer = peerType => peerType == "PUSH"
    };

    /// <summary>ROUTER: accepts DEALER, REQ, or ROUTER peers; advertises an identity (0025).</summary>
    public static ZSocketType Router { get; } = new()
    {
        Name = "ROUTER",
        AcceptsPeer = peerType => peerType is "DEALER" or "REQ" or "ROUTER",
        AdvertisesIdentity = true
    };

    /// <summary>PUB: accepts a peer advertising SUB.</summary>
    public static ZSocketType Pub { get; } = new()
    {
        Name = "PUB",
        AcceptsPeer = peerType => peerType == "SUB"
    };

    /// <summary>SUB: accepts a peer advertising PUB.</summary>
    public static ZSocketType Sub { get; } = new()
    {
        Name = "SUB",
        AcceptsPeer = peerType => peerType == "PUB"
    };

    /// <summary>XPUB: accepts SUB, XSUB, or XPUB peers (the XPUB/XSUB interchange).</summary>
    public static ZSocketType XPub { get; } = new()
    {
        Name = "XPUB",
        AcceptsPeer = peerType => peerType is "SUB" or "XSUB" or "XPUB"
    };

    /// <summary>XSUB: accepts PUB, XPUB, or XSUB peers.</summary>
    public static ZSocketType XSub { get; } = new()
    {
        Name = "XSUB",
        AcceptsPeer = peerType => peerType is "PUB" or "XPUB" or "XSUB"
    };
}

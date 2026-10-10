using Xunit;
using ZmqSharp.Patterns;

namespace ZmqSharp.Tests.Sockets;

/// <summary>
/// Locks the built-in compatibility matrix (0015 section 2.2): compatibility
/// is declared per endpoint, so each <see cref="ZSocketTypes"/> member's
/// <see cref="ZSocketType.AcceptsPeer"/> predicate is asserted against every
/// built-in name plus a foreign name. This is the explicit record of the
/// asymmetric matrix (REQ accepts REP, PUSH accepts PULL, PUB accepts SUB, ...)
/// that the NetMQ interop suite locks end-to-end.
/// </summary>
public sealed class ZSocketTypeTests
{
    private static readonly string[] AllBuiltInNames =
    [
        "PAIR", "DEALER", "REQ", "REP", "PUSH", "PULL", "ROUTER", "PUB", "SUB", "XPUB", "XSUB"
    ];

    [Theory]
    [InlineData("PAIR")]
    [InlineData("DEALER")]
    [InlineData("REQ")]
    [InlineData("REP")]
    [InlineData("PUSH")]
    [InlineData("PULL")]
    [InlineData("ROUTER")]
    [InlineData("PUB")]
    [InlineData("SUB")]
    [InlineData("XPUB")]
    [InlineData("XSUB")]
    public void AcceptsPeer_MatchesMatrix_ForEveryBuiltInName(string localName)
    {
        var type = BuiltIn(localName);

        foreach (var peerName in AllBuiltInNames)
            Assert.Equal(Matrix.Accepts(localName, peerName), type.AcceptsPeer(peerName));
    }

    [Theory]
    [InlineData("PAIR")]
    [InlineData("DEALER")]
    [InlineData("REQ")]
    [InlineData("REP")]
    [InlineData("PUSH")]
    [InlineData("PULL")]
    [InlineData("ROUTER")]
    [InlineData("PUB")]
    [InlineData("SUB")]
    [InlineData("XPUB")]
    [InlineData("XSUB")]
    public void AcceptsPeer_RejectsForeignName(string localName)
    {
        // A name outside the built-in set is a custom type; a built-in endpoint
        // never accepts one (custom types interop only between ZmqSharp
        // endpoints advertising the same name, 0015 section 2.3).
        Assert.False(BuiltIn(localName).AcceptsPeer("CUSTOM"));
    }

    [Fact]
    public void CustomType_AcceptsPeer_SameNameOnly()
    {
        var type = new ZSocketType
        {
            Name = "FOO",
            AcceptsPeer = peerType => peerType == "FOO"
        };

        Assert.True(type.AcceptsPeer("FOO"));
        Assert.False(type.AcceptsPeer("PAIR"));
    }

    [Fact]
    public void Name_Empty_Throws()
    {
        var act = () => new ZSocketType { Name = "", AcceptsPeer = _ => true };

        var ex = Assert.Throws<ArgumentException>(act);
        Assert.Contains("not be empty", ex.Message);
    }

    [Fact]
    public void Name_TooLong_Throws()
    {
        var act = () => new ZSocketType { Name = new string('X', 256), AcceptsPeer = _ => true };

        var ex = Assert.Throws<ArgumentException>(act);
        Assert.Contains("255", ex.Message);
    }

    [Fact]
    public void Name_NonAscii_Throws()
    {
        var act = () => new ZSocketType { Name = "FOÖ", AcceptsPeer = _ => true };

        var ex = Assert.Throws<ArgumentException>(act);
        Assert.Contains("ASCII", ex.Message);
    }

    private static ZSocketType BuiltIn(string name) => name switch
    {
        "PAIR" => ZSocketTypes.Pair,
        "DEALER" => ZSocketTypes.Dealer,
        "REQ" => ZSocketTypes.Req,
        "REP" => ZSocketTypes.Rep,
        "PUSH" => ZSocketTypes.Push,
        "PULL" => ZSocketTypes.Pull,
        "ROUTER" => ZSocketTypes.Router,
        "PUB" => ZSocketTypes.Pub,
        "SUB" => ZSocketTypes.Sub,
        "XPUB" => ZSocketTypes.XPub,
        "XSUB" => ZSocketTypes.XSub,
        _ => throw new ArgumentOutOfRangeException(nameof(name), name)
    };

    /// <summary>The libzmq per-end acceptance matrix (the ex-IsCompatibleSocketType switch).</summary>
    private static class Matrix
    {
        public static bool Accepts(string local, string peer) => local switch
        {
            "PAIR" => peer == "PAIR",
            "DEALER" => peer is "DEALER" or "REP" or "ROUTER",
            "ROUTER" => peer is "DEALER" or "REQ" or "ROUTER",
            "REQ" => peer == "REP",
            "REP" => peer is "REQ" or "DEALER",
            "PUSH" => peer == "PULL",
            "PULL" => peer == "PUSH",
            "PUB" => peer is "SUB" or "XSUB",
            "SUB" => peer is "PUB" or "XPUB",
            "XPUB" => peer is "SUB" or "XSUB" or "XPUB",
            "XSUB" => peer is "PUB" or "XPUB" or "XSUB",
            _ => false
        };
    }
}

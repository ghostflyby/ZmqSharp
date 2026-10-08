namespace ZmqSharp.Security;

/// <summary>
/// Server-side PLAIN credential check (RFC 24). The delegate keeps the
/// mechanism AOT-safe - an authenticator is configured explicitly, never
/// discovered through reflection. Credentials arrive as the decoded UTF-8
/// username field and the raw bytes of the password field in HELLO.
/// </summary>
public delegate bool ZPlainAuthenticator(string username, ReadOnlySpan<byte> password);

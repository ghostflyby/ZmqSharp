namespace ZmqSharp.Security;

/// <summary>
/// Server-side PLAIN credential check (RFC 27). The delegate keeps the
/// mechanism AOT-safe - an authenticator is configured explicitly, never
/// discovered through reflection. Credentials arrive as the decoded UTF-8
/// text of the HELLO Username property and the raw bytes of the Password
/// property (the shared metadata parser decodes both as text).
/// </summary>
public delegate bool ZPlainAuthenticator(string username, ReadOnlySpan<byte> password);

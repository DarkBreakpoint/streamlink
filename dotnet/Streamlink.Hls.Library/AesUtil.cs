using System;
using System.IO;
using System.Security.Cryptography;
using System.Buffers.Binary;
using System.Buffers;

namespace Streamlink.Hls.Library;

public static class AesUtil
{
    public static byte[] CreateIv(long sequenceNumber)
    {
        byte[] iv = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(iv.AsSpan(8), sequenceNumber);
        return iv;
    }

    public static byte[] Decrypt(ReadOnlySpan<byte> encryptedData, byte[] key, byte[] iv)
    {
        using var aes = Aes.Create();
        aes.Key = key;
        aes.IV = iv;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.PKCS7;

        using var decryptor = aes.CreateDecryptor();
        return decryptor.TransformFinalBlock(encryptedData.ToArray(), 0, encryptedData.Length);
    }

    // Optimized decryption reusing buffer
    // Decrypts in-place if possible or writes to output span
    public static int DecryptInto(ReadOnlySpan<byte> encryptedData, Span<byte> output, byte[] key, byte[] iv)
    {
        using var aes = Aes.Create();
        aes.Key = key;
        aes.IV = iv;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.PKCS7;

        using var decryptor = aes.CreateDecryptor();

        // TryDecrypt is available in newer .NET versions
        if (decryptor.TryTransformFinalBlock(encryptedData, output, out int bytesWritten))
        {
            return bytesWritten;
        }
        throw new InvalidOperationException("Failed to decrypt into span.");
    }

    public static Stream CreateDecryptingStream(Stream inner, byte[] key, byte[] iv)
    {
        var aes = Aes.Create();
        aes.Key = key;
        aes.IV = iv;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.PKCS7;

        var decryptor = aes.CreateDecryptor();

        return new CryptoStreamWrapper(inner, decryptor, CryptoStreamMode.Read, aes);
    }

    private class CryptoStreamWrapper : CryptoStream
    {
        private readonly IDisposable _aes;
        private readonly IDisposable _transform;

        public CryptoStreamWrapper(Stream stream, ICryptoTransform transform, CryptoStreamMode mode, IDisposable aes)
            : base(stream, transform, mode)
        {
            _transform = transform;
            _aes = aes;
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                _transform.Dispose();
                _aes.Dispose();
            }
        }
    }
}

using System.Security.Cryptography;
using System.Text;

namespace SandFlow.Server;

public static class AdmissionPolicy
{
    public static bool Allows(bool publicAdmission, bool authenticatedPreview, string expected, string supplied)
    {
        if (publicAdmission || authenticatedPreview) return true;
        if (expected.Length < 64 || supplied.Length != expected.Length) return false;
        return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(expected), Encoding.UTF8.GetBytes(supplied));
    }
}

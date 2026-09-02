// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

namespace Elekto.Mcp.Sql.Data;

/// <summary>
/// Turns the raw <c>sys.columns</c> triple (type name, max_length, precision/scale) into the
/// declaration a human would write, plus the length in the unit a human would mean.
/// </summary>
/// <remarks>
/// <para>
/// <c>sys.columns.max_length</c> is documented in BYTES. For the Unicode string types that is twice
/// the declared length, so a <c>nvarchar(250)</c> column reports 500. Reading that 500 as characters
/// is an easy and expensive mistake: it does not fail, it silently produces a conclusion that is
/// wrong by a factor of two, and nothing downstream contradicts it.
/// </para>
/// <para>
/// So the raw <c>max_length</c> is still reported — it is what the catalog says, and fidelity to the
/// catalog is worth keeping — but it is reported next to <c>max_length_chars</c> and
/// <c>type_declaration</c>, either of which answers the question directly and cannot be misread.
/// </para>
/// </remarks>
public static class SqlTypeFormatter
{
    /// <summary>The value <c>sys.columns.max_length</c> uses for the MAX types.</summary>
    private const short MaxLengthSentinel = -1;

    /// <summary>Types whose <c>max_length</c> is bytes but whose declaration is in characters, two bytes each.</summary>
    private static readonly HashSet<string> UnicodeTextTypes =
        new(StringComparer.OrdinalIgnoreCase) { "nchar", "nvarchar", "sysname" };

    /// <summary>Types whose declaration is in characters, one byte each.</summary>
    private static readonly HashSet<string> AnsiTextTypes =
        new(StringComparer.OrdinalIgnoreCase) { "char", "varchar" };

    /// <summary>Types declared in bytes.</summary>
    private static readonly HashSet<string> BinaryTypes =
        new(StringComparer.OrdinalIgnoreCase) { "binary", "varbinary" };

    /// <summary>Types declared with precision and scale.</summary>
    private static readonly HashSet<string> PrecisionScaleTypes =
        new(StringComparer.OrdinalIgnoreCase) { "decimal", "numeric" };

    /// <summary>Types declared with a fractional-seconds scale only.</summary>
    private static readonly HashSet<string> ScaleOnlyTypes =
        new(StringComparer.OrdinalIgnoreCase) { "datetime2", "time", "datetimeoffset" };

    /// <summary>
    /// The declared length of a string or binary column, in the unit its declaration uses:
    /// characters for the text types, bytes for the binary ones. <c>-1</c> means MAX.
    /// Returns <c>null</c> for every type that has no declared length, so a caller that finds a
    /// number here knows the number means something.
    /// </summary>
    public static int? GetMaxLengthChars(string dataType, short maxLength)
    {
        if (string.IsNullOrWhiteSpace(dataType)) return null;

        if (maxLength == MaxLengthSentinel &&
            (UnicodeTextTypes.Contains(dataType) || AnsiTextTypes.Contains(dataType) || BinaryTypes.Contains(dataType)))
            return MaxLengthSentinel;

        if (UnicodeTextTypes.Contains(dataType)) return maxLength / 2;
        if (AnsiTextTypes.Contains(dataType) || BinaryTypes.Contains(dataType)) return maxLength;

        return null;
    }

    /// <summary>
    /// The column type as it would be written in a CREATE TABLE: <c>nvarchar(250)</c>,
    /// <c>nvarchar(max)</c>, <c>decimal(18,6)</c>, <c>datetime2(3)</c>, <c>char(3)</c>, <c>float</c>.
    /// Unambiguous by construction — there is no unit left for a reader to guess at.
    /// </summary>
    public static string Format(string dataType, short maxLength, byte precision, byte scale)
    {
        if (string.IsNullOrWhiteSpace(dataType)) return string.Empty;

        var length = GetMaxLengthChars(dataType, maxLength);
        if (length is not null)
            return length == MaxLengthSentinel
                ? $"{dataType}(max)"
                : $"{dataType}({length})";

        if (PrecisionScaleTypes.Contains(dataType))
            return $"{dataType}({precision},{scale})";

        // datetime2, time and datetimeoffset default to scale 7; spelling it out only adds noise.
        if (ScaleOnlyTypes.Contains(dataType))
            return scale == 7 ? dataType : $"{dataType}({scale})";

        // float(53) is the default and is written plain; float(24) is meaningfully different.
        if (dataType.Equals("float", StringComparison.OrdinalIgnoreCase))
            return precision == 53 ? dataType : $"{dataType}({precision})";

        return dataType;
    }
}

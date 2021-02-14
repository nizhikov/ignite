namespace Apache.Ignite.Core.Binary
{
    using System;
    using System.Globalization;
    using System.Text;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Implementation of <code>IBinaryNameMapper</code> that handles differs in Java and .Net naming conventions.
    /// </summary>
    /// <seealso cref="https://www.oracle.com/java/technologies/javase/codeconventions-namingconventions.html"/>
    /// <seealso cref="https://docs.microsoft.com/en-us/dotnet/standard/design-guidelines/names-of-namespaces"/>
    public class BinaryBasicNameMapperForJava: BinaryBasicNameMapper
    {
        /// <summary>
        /// Domain to be removed for .Net style name or added as a first part of Java type name.
        /// Java and .Net assumes usage of different naming conventions.
        /// Java package name first part is a domain: com, org, ru, etc. and second part is a company name: acme, company, etc.
        /// .Net naming conventions don't use domain and start with a company name.
        /// </summary>
        public string JavaDomain { get; set; }

        /// <summary>
        /// Perform mapping to java naming convention e.g `Com.Company.Class` maps to `com.company.Class`.
        /// </summary>
        /// <param name="name">Type name with the namespace.</param>
        /// <param name="javaDomain">Type name with the namespace.</param>
        /// <returns></returns>
        private static string DoForceJavaNamingConventions(string name, string javaDomain)
        {
            bool toUpper = false;

            if (javaDomain != null)
            {
                if (name.StartsWith(javaDomain, StringComparison.OrdinalIgnoreCase))
                {
                    name = name.Substring(javaDomain.Length);
                    toUpper = true;
                }
                else
                    name = javaDomain + name;
            }

            var arr = name.ToCharArray();

            var nameStart = 0;

            for (int i = 0; i < arr.Length; i++)
            {
                if (arr[i] == '.')
                {
                    arr[nameStart] = toUpper
                        ? Char.ToUpper(arr[nameStart], CultureInfo.CurrentCulture)
                        : Char.ToLower(arr[nameStart], CultureInfo.CurrentCulture);
                    nameStart = i + 1;
                }
            }

            return new string(arr);
        }

        /// <summary>
        /// Gets the type name.
        /// </summary>
        public override string GetTypeName(string name)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "typeName");

            var parsedName = TypeNameParser.Parse(name);

            if (parsedName.Generics == null)
            {
                // Generics are rare, use simpler logic for the common case.
                var res = IsSimpleName ? parsedName.GetName() : parsedName.GetNameWithNamespace();

                if (!IsSimpleName)
                {
                    res = DoForceJavaNamingConventions(res, 
                        JavaDomain == null ? null : JavaDomain + '.');
                }

                var arr = parsedName.GetArray();

                if (arr != null)
                {
                    res += arr;
                }

                return res;
            }

            var nameFunc = IsSimpleName
                ? (Func<TypeNameParser, string>) (x => x.GetName())
                : x => DoForceJavaNamingConventions(x.GetNameWithNamespace(),
                    JavaDomain == null ? null : JavaDomain + '.');

            return BuildTypeName(parsedName, new StringBuilder(), nameFunc).ToString();
        }
    }
}
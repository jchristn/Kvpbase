using System;
using System.Collections;
using System.Collections.Generic;
using System.IO; 

namespace Kvpbase.Core
{
    /// <summary>
    /// A Kvpbase user.
    /// </summary>
    public class UserMaster
    {
        #region Public-Members

        /// <summary>
        /// The ID of the user.
        /// </summary>
        public int? UserMasterId { get; set; }

        /// <summary>
        /// The ID of the node to which the user is mapped (set to 0 if none).
        /// </summary>
        public int? NodeId { get; set; } 

        /// <summary>
        /// The first name.
        /// </summary>
        public string FirstName { get; set; }

        /// <summary>
        /// The last name.
        /// </summary>
        public string LastName { get; set; }

        /// <summary>
        /// The company name.
        /// </summary>
        public string CompanyName { get; set; }

        /// <summary>
        /// Email address.
        /// </summary>
        public string Email { get; set; }

        /// <summary>
        /// Password.
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Cellular phone number.
        /// </summary>
        public string Cellphone { get; set; }

        /// <summary>
        /// Address line 1.
        /// </summary>
        public string Address1 { get; set; }

        /// <summary>
        /// Address line 2.
        /// </summary>
        public string Address2 { get; set; }

        /// <summary>
        /// City.
        /// </summary>
        public string City { get; set; }

        /// <summary>
        /// State.
        /// </summary>
        public string State { get; set; }

        /// <summary>
        /// Postal code.
        /// </summary>
        public string PostalCode { get; set; }

        /// <summary>
        /// Country (recommend using the ISO A3 country code).
        /// </summary>
        public string Country { get; set; }
         
        /// <summary>
        /// The user's home directory.  If null, a directory will be created under the default storage directory.
        /// </summary>
        public string HomeDirectory { get; set; }
          
        /// <summary>
        /// GUID for the user.
        /// </summary>
        public string Guid { get; set; }

        /// <summary>
        /// Indicates if the account is active (1) or disabled (0).
        /// </summary>
        public int? Active { get; set; }

        /// <summary>
        /// The timestamp from when the user was created.
        /// </summary>
        public DateTime? Created { get; set; }

        /// <summary>
        /// The timestamp from when the user was last updated.
        /// </summary>
        public DateTime? LastUpdate { get; set; }

        /// <summary>
        /// The timestamp for when the account should be considered expired.
        /// </summary>
        public DateTime? Expiration { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public UserMaster()
        {

        }

        /// <summary>
        /// Retrieve the list of users from a file.
        /// </summary>
        /// <param name="filename">The full path and filename.</param>
        /// <returns>List of UserMaster.</returns>
        public static List<UserMaster> FromFile(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));
            if (!Common.FileExists(filename)) throw new FileNotFoundException(nameof(filename));

            Console.WriteLine(Common.Line(79, "-"));
            Console.WriteLine("Reading users from " + filename);
            string contents = Common.ReadTextFile(@filename);

            if (String.IsNullOrEmpty(contents))
            {
                Common.ExitApplication("UserMaster", "Unable to read contents of " + filename, -1);
                return null;
            }

            Console.WriteLine("Deserializing " + filename);
            List<UserMaster> ret = null;

            try
            {
                ret = Common.DeserializeJson<List<UserMaster>>(contents);
                if (ret == null)
                {
                    Common.ExitApplication("UserMaster", "Unable to deserialize " + filename + " (null)", -1);
                    return null;
                }
            }
            catch (Exception)
            { 
                Common.ExitApplication("UserMaster", "Unable to deserialize " + filename + " (exception)", -1);
                return null;
            }

            return ret;
        }
         
        #endregion

        #region Public-Methods
         
        #endregion

        #region Private-Methods

        #endregion
    }
}

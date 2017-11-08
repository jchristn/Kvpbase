using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kvpbase
{
    public class FsHelper
    {
        public static bool ContainsUnsafeFsChars(MoveRequest currMove)
        {
            if (currMove == null) return true;
            if (Common.ContainsUnsafeCharacters(currMove.FromContainer)) return true;
            if (Common.ContainsUnsafeCharacters(currMove.ToContainer)) return true;
            if (Common.ContainsUnsafeCharacters(currMove.MoveFrom)) return true;
            if (Common.ContainsUnsafeCharacters(currMove.MoveTo)) return true;
            return false;
        }

        public static bool ContainsUnsafeFsChars(RenameRequest currRename)
        {
            if (currRename == null) return true;
            if (Common.ContainsUnsafeCharacters(currRename.ContainerPath)) return true;
            if (Common.ContainsUnsafeCharacters(currRename.RenameFrom)) return true;
            if (Common.ContainsUnsafeCharacters(currRename.RenameTo)) return true;
            return false;
        }

        public static bool ContainsUnsafeFsChars(Obj currObj)
        {
            if (currObj == null) return true;
            if (currObj.ContainerPath != null && currObj.ContainerPath.Count > 0)
            {
                if (Common.ContainsUnsafeCharacters(currObj.ContainerPath)) return true;
            }
            if (!String.IsNullOrEmpty(currObj.Key))
            {
                if (Common.ContainsUnsafeCharacters(currObj.Key)) return true;
            }
            return false;
        }

    }
}

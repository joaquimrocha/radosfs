# Architecture {#arch}

RadosFs was designed taking into account scalability and flexibility so it
offers similar ways of accomplishing the same things, although each way might
mean, for example, that the succeeding operations will be faster.

This section aims to explain some of those design decisions and how you can
leverage those to achieve your goals in the best way possible.


\section radosfs Filesystem

The Filesystem class is the object that represents the whole filesystem. It
corresponds to a Ceph cluster and has many of the basic objects necessary
for the cluster-related operations (pools, caches, etc.).

\subsection genericworkers Generic Worker Threads

A Filesystem keeps a number of threads that can be used to perform generic work,
for this reason they're called *generic worker threads*.
Most of the parallel work performed by some methods -- such as Dir::find or
File::write -- uses the generic worker threads.

The number of generic worker threads can be configured by
Filesystem::setNumGenericWorkers and retrieved by Filesystem::numGenericWorkers.
Generic worker threads are only launched when an operation needs them, after
that they stay alive until the Filesystem instance is destroyed.
Thus, when setting the number of generic worker threads, they are only changed
(created or destroyed) if at least one thread has been launched.
By default, the number of generic worker threads is **4**.

\section dir Directories

Directories are represented by the Dir class. Internally, they are represented
by two objects in a metadata pool of the cluster:
  * **Path object:** an object whose name is the absolute path of the directory;
  * **Inode object:** an object whose name is a uuid.


\subsection dirpathobj Directory path object

The path object uses the absolute path as the name so statting a directory is
done quickly without having to navigate through a hierarchy of paths. Besides
serving the direct stat, this objects only holds a reference to the inode
object.


\subsection dirinodeobj Directory inode object

The inode object is the one that holds the data (entries, metadata, xattributes,
etc.).
Each directory keeps a log of entry-related operations which is stored as the
contents of the inode object. When adding or deleting a subdirectory or a file
to a directory, an entry is appended to its log. The following example
represents a possible log for a directory:

    +name="subdir-1/"
    +name="subdir-2/"
    +name="file-1"
    +name="file-2"
    -name="file-2"
    +name="subdir-3/"
    -name="subdir-2/"

The "+name" entries mean that an entry was added, analogously, "-name" entries
mean that an entry was deleted. Thus, reading this log, we can find out that the
directory in question has two subdirectories (*subdir-1/* and *subdir-3/*) and
one file (*file-1*) since *subdir-2/* and *file-2* were deleted.

The reason why the directories keep a log instead of a clean list of their
contents is that this way their contents can be read iteratively. That is, a
Dir object keeps track of what portion of the log has been read so far, hence,
when updating its contents list, it does not have to reread the whole contents
to know what has changed.

As you might have noticed, directory and file paths are distinguished by having
a trailing slash (in the case of directories).

\subsection dirmtd Entries metadata

Directory objects support adding metadata associated with an entry. This
metadata is also kept in the log. An example would be:

    +name="subdir-1/" +mtd."my metadata"="my value"
    +name="subdir-3/" +mtd."last-user-listing"="john doe"
    +name="file-1" +mtd."last-user-reading"="john doe"
    +name="subdir-1/" -mtd."my metadata"

The lines with "+mtd." and "-mtd." mean that metadata has been added or
removed to the entry indicated by "+name", respectively.

Thus, in the example above, the directory *subdir-3/* and the file *file-1/*
would have metadata (*last-user-listing* and *last-user-reading*, respectively),
while the directory *subdir-1/* would have no metadata because it has been
deleted.


\subsection dircompaction Directory compaction

After the examples above, it becomes obvious that the number of entries and
metadata in a directory log may become greater than what it really has.
Eventually, adding an entry and removing it repeatedly would create a long log
for representing an empty directory. This means wasted space and a longer time
for reading the contents of a directory.

For this reason, directories can be compacted. Compaction works by getting the
contents of a directory at a certain time and rewriting a clean log as if they
were added at that time.

Directory compaction can be directly called by using Dir::compact or
automatically depending on the *compact ratio* which is calculated by the actual
number of entries dividing by the number of lines in the log.

The *compact ratio* can be set in a Filesystem instance using
Filesystem::setDirCompactRatio and will affect all the Dir objects associated
with it. By default, the ratio that triggers the automatic compaction is **0.2**
which means that if the *compact ratio* is lesser than or equal to that value,
than the directory is compacted. Note that this automatic compaction (when
needed) is only triggered by the Dir::update method. Certainly, setting a
*compact ratio* or zero will result in the automatic compaction never being
called.


\subsection dircache Directory caching

Since listing directories is something that might be repeated throughout the use
of the library (by one or more users), then, besides the iterative reading of
the entries presented above, it is useful to keep a cache of the directories
and their entries.

For this reason, each Dir objects instantiated with the same path, have a common
object (DirCache) that contains the list of entries in the directory and stays
cached even when all instances of the Dir in question are destroyed.

Since the number of entries in a directory can be from just a few to a very
large number, the cache has a maximum size that is calculated by the sum of the
number of directories and their entries. This cache is implemented using a
priority queue. Upon reaching its limit, the cache cleans 80% of its contents
(discarding the least recently used directories).

The maximum size mentioned above can be set or retrieved by the
Filesystem::setDirCacheMaxSize and Filesystem::dirCacheMaxSize, respectively. By
default, this value is **1 million**.


\subsubsection skipdircache Non-cacheable directories

By default, all directories instantiated by the user will be cached. However,
this might not be the desired behavior for some use-cases. For example, when
calling the Dir::find method, subdirectories will be instantiated recursively
but there is no reason why they should be cached (because the user is looking
for contents, not trying to list them).
For this and other cases, a non-cacheable directory contructor is also provided:
Dir::Dir(Filesystem* radosFs,const std::string &path,bool cacheable) , just set
the cacheable argument to *false* and this directory will not be cached.

\subsubsection listdir Listing a directory

The method used to obtain a directory's entries is Dir::entryList. However, this
method does not read the entries that are effectively stored in the objects in
the cluster but rather takes them from the cached list in memory. This means
that in order to get the latest list of entries, Dir::update needs to be called
right before Dir::entryList. See the \ref updateobjs section.

\subsubsection dirtmid TMId in directories

Directories' [*TMId*](\ref usetmid) is set as an omap value asynchronously and
only in directories that have it enabled.

\section Files

Files are represented by the File class. Internally, they are very more complex
than directories. Files are represented by one object in a data pool of the
cluster and an entry in a directory object's omap.
The file's object in the data pool is called in the **inode object**: an object
whose name is a uuid.

\subsection filedirentry File path entry

Unlike directories, files do not have a dedicated object with their full path,
so there needs to be a way to find out if a file exists and which inode object
corresponds to it. This is done by having an entry in the file's parent
directory's omap called the *file path entry*. This entry holds the information
of what inode's name and pool correspond to the file, as well as the
permissions, owner, group, chunk size, inline buffer size and creation time.

Here is an example of a file entry in its parent directory's omap:

    link="add82f82-32cc-434a-9562-3ffe574f8c18" pool='data-pool' uid="0" gid="0" time="1426860288.157990864" mode="100744" inline='4096' rfs.stripe='134217728'

\subsection fileinodeobj File inode object

As with the directory, the inode object is the one that holds the data, however
in this case the data is really the unformatted data written by the user.

The inode object also takes other important information as xattributes. Those
are the size of the file and a back link pointing to the path of the file that
this inode belongs to.

\subsubsection fileinodelazycreation Lazy creation

Although the name of the inode object corresponding to a file is generated when
creating the file, the inode object itself is not created at this time.
Since a file inode object exists to keep that file's contents and xattributes,
the object is only created when those are set, that is, on a write, truncate or
xattribute setting operation.

This is necessary to avoid spending time and space with inode objects that may
never be needed -- see \ref inlinefiles.

\subsubsection filechunks File chunks

File data is divided and written in different file chunks. Each chunk of data
corresponds to an object in a data pool. The first chunk object is the very
inode object (called *base inode*) while the remaining chunks are written into
other objects with the same name as the inode but with a suffix indicating their
index (in hexadecimal). E.g.:

    b7694e2d-a0b3-4514-b89c-976f50d8e181#00000001
    b7694e2d-a0b3-4514-b89c-976f50d8e181#00000002
    b7694e2d-a0b3-4514-b89c-976f50d8e181#00000003
    ...
    b7694e2d-a0b3-4514-b89c-976f50d8e181#0000000A
    ...

Unlike the base inode object, all the other chunk objects do not have any other
data than their contents.

The size of a file's chunks can be set per file when calling its File::create
method. If the *chunkSize* value not set (or is 0), then the global chunk size
will be used. To set or get the default chunk size, use
Filesystem::setFileStripeSize and Filesystem::fileStripeSize, respectively. The
default value for the global file chunk size is **128 MB**.

\subsection inlinefiles Inline files

Creating full inode objects may not be very efficient for use-cases where files
are rather small. For this reason, inline files are also supported.
In RadosFs, inline files are files that can keep a limited amount of data
outside of their inode objects. Like the file path entry, this data is stored in
the file's parent directory's omap.

Since there is a limit on this inline buffer, after the buffer is full,
the remaining data is written in the file's inode object. This is completely
transparent to the user, who just calls the regular File::write method -- it is
up to the library to know in which place should the data be stored.

The size of the inline buffer can be configured per file when calling the
File::create method and can be obtained from a File instance by using
File::inlineBufferSize. By default, the file inline buffer size is **4 KB**.

Note that, since all files have a modification time, which is set as an
xattribute on the inode, when using an inline file, its modification time is
stored as part of a header in its omap's value.
Although the modification time only takes 8 bytes, the header is 64 bytes long,
in order to cover any future needs for storing data in the header (and naturally
this size is independent from the inline buffer size that will hold the
contents).


\subsection filelocking File locking

When instancing a File object, no lock is taken on that file, moreover, a user
can write a large amount of data calling File::write once or several times
(dividing the data), so locking the file object on each write operation could
become very inefficient.
To solve this issue, File instances implement a timed-locks strategy: When the
first file writing operation is called (by writing, truncating or deleting
the file), a lock is automatically set with a duration of 120 seconds. When
calling one of those operations again, they check if there's an existing lock
for the file, in case it exists, if the lock is about to expired, then it is
renewed, otherwise it is kept.

The above strategy works but if a locking operation finishes well under the lock
duration, then the lock would be left blocking other clients who would need it
to timeout in order to effectively perform their own operations. To solve this,
there is a dedicated thread constantly checking all the files' operations for
idle locks. If a lock has been idle for more than **200 milliseconds**, it is
broken from the mentioned thread. This way, only in the case of a client
crashing would the file's objects be locked for the remaining time of the
duration of the lock.

\subsection fileinode FileInode objects

Each File instance uses a FileInode instance internally for calling the
operations related to the file's data (writing, reading, truncating, etc.).

Registered FileInode objects will have a back link associated with them. This is
the file path associated with them and can be retrieved using
FileInode::getBackLink. This is important mainly for the *radosfsck* tool which
uses information like this to check the integrity of the filesystem.

\subsubsection fileio Common file operations object

Similar to the cached directories, each FileInode instance of the same inode
object also share a common object (FileIO) which is the one that implements the
file operations and also keeps track and controls the asynchronous operations.
However, unlike directory cache objects, FileIO instances are discarded when no
FileInode instance (or File instance, by association) needs them anymore.


\section updateobjs File and directory updating

When a File or Dir object is intantiated, it reads some of the corresponding
data or metadata from the cluster, for example the permissions. This data is not
updated every time an operation needs it, for instance, the permissions are not
updated from the cluster every time File::write is read, doing such would be
very ineficient. In a similar way, Dir objects keep the last read entries of the
directory so when calling Dir::entryList, it does retrieves this list from a
cache, instead of reading the real data from the cluster.
Of course, getting the latest data from the cluster is of extreme importance, so
File and Dir objects implement an *update* method which updates the status of
the objects according to the latest data in the cluster. In the case of
directories, calling *update* also reads their entries.
Traditionally, the update operation can be seen as reopening a file.


\section archlinks Links

By design, only symbolic links are supported and there cannot be a link to a
link. To learn how links can be used, please read the \ref uselinks section.

Internally, links are essencially files pointing to a path instead of an
inode (even though they can be instantiated as Dir objects -- when they are
links to directories).

Links only exist as an entry in their parent directory log and omap, they never
have an inode objects.
Here is an example of the omap entry for a link:

    link="/file-55" uid="0" gid="0" time="1426860288.193859311" mode="120744" inline='0' rfs.stripe='134217728'

Compare the entry above with the example in the \ref filedirentry to check the
differences.


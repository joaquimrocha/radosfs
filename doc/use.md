# Using libradosfs {#use}

This page explains how to use the *libradosfs* to cover filesystem-related
use-cases.

\note Instead of throwing exceptions, most of the methods in the library will
      return a code with the result of their operation: **0** if the operation
      was successful, a **negative** error code otherwise (taken from
      *errno.h*). Throughout the examples, however, some of these return codes
      are deliberatly ignored for the simplicity sake, so remember to always
      check the return codes of your calls when writing "real" code.

\section usebegin Beginning

The first step for using *libradosfs* is to instanciate a Filesystem object which
represents the whole filesystem and is associated with a Ceph cluster.

\subsection radosfsinit Initialize the filesystem

After creating an instance of Filesystem, it is not yet ready to be used since
it needs to be initialized with a Ceph cluster.
For that, the Filesystem::init method is used:

    radosfs::Filesystem fs;
    int result = fs.init("jay", "/path/to/conf/file");
    
    if (result != 0)
    {
      fprintf(stderr, "Could not initialize cluster.");
      exit(result);
    }

Note that usually, only a single instance of Filesystem is needed for most
use-cases.

\subsection radosfsaddpools Adding pools

After successfully initializing the Filesystem instance, it still does not know
which pools should it consider, so at least one metadata and one data pool need
to be added. Metadata pools will store the data related to directories whereas
data pools will only store data associated with the files' contents.

Pools are added by associating their name with a path prefix.

    fs.addMetadata("mtd-pool", "/");
    fs.addData("data-pool", "/", 1024);
    fs.addData("test-pool", "/test/", 1024);

In the example above, a metadata pool called "mtd-pool" and a data pool called
"data-pool" will be associated with all the directories and file objects in the
filesystem, since all paths start with the root "/". Nonetheless, the pool named
"test-pool" will only be associated with files that begin with "/test/".

The 1024 values given to the *addData* methods above are the maximum size
allowed for the files in those pools.

\note Keep in mind that the mentioned methods do not, in fact, create any pools,
      they only associate the given pools with the prefixes. So the pools have
      to exist before they are added to the filesystem.

\note Aligned pools are supported transparently without any special care
      required from the user side.

\subsection usesetids Setting the user and group ids

*libradosfs* keeps a user id and a group id which are used as if the operations were
performed by that user or group. For example, when creating a directory or file,
the permissions for writing in a parent directory are checked against the user
and group id values which will also become the owner user and group, if the
creation is successful.

The user and group ids can be set using Filesystem::setIds and retrieved
collectively using Filesystem::getIds or individually using Filesystem::uid and
Filesystem::gid.

Those values are thread local so the same instance of Filesystem can be used in
different threads without sharing the same user and group ids.

\note Although the user and group id values are used for checking the
permissions, *libradosfs* as no authentication in place for checking if the client
who sets those values, is allowed to do so. This means that 

By default, the user and group id is set to the *root*.

\subsection usecommon Common directory and file operations

Both the File and Dir classes have the same base class FsObj the way to use them
is very similar.

\subsubsection usecommoninstantiation Instantiating files and directories

File and Dir classes can be instantiated using their own constructors which
require the corresponding Filesystem as an argument, for example:

    ...
    radosfs::Dir dir(&fs, "/my-dir/");
    radosfs::File file(&fs, "/my-file");

Note that directory paths always end up in a slash ("/") contrary to file paths.
However, for simplicity, both versions are supported in all the constructors,
that is, in the example above, we could use "/my-dir" as the directory path
and even "/my-file/" for the file path.
Either way, instantiating a Dir or File directly assumes prior knowledge from
the user that the given paths are the right type and this might not always be
the case.

To solve this, there's a factory method in the Filesystem that returns a FsObj
instance that will have the right type from the object in the filesystem.
For example:

    ...
    radosfs::FsObj *obj = fs.getFsObj("/user-tmp");
    
    if (!obj)
    {
        printf("Error...\n");
    }
    else if (obj->isDir())
    {
        printf("%s is a directory!\n", obj->path().c_str());
    }
    else // or obj->isFile()
    {
        printf("%s is a file!\n", obj->path().c_str());
    }

\subsubsection usecreation Creation

Before calling the typical operations on File or Dir objects, the
corresponding objects in the filesystem have to be created. This can be done
with their *create* methods.

    radosfs::Dir dir(&fs, "/my-dir/");
    dir.create();

    radosfs::File file(&fs, "/my-file");
    file.create();

\subsubsection usepermissions Permissions

Files and directories have permissions settings that, as mentioned before, will
be checked with the current *user id* and *group id* when performing
operations. Thus, to avoid eventual permissions issues, remember to change the
\ref Filesystem::setIds "filesystem's ids" to the user/group that should be
performing the operation in question.

These permissions can be changed using the \ref FsObj::chmod "chmod" method or
when creating the objects. The following example will create a directory that
any user can read or write:

    radosfs::Dir dir(&fs, "/public-dir/");
    dir.create(S_IRWXU | S_IRWXG | S_IRWXO);

Undoubtedly, the same could be done for a file.

Sometimes, it is necessary to check directly whether an object is readable or
writable by the current user/group. This can be done using the
FsObj::isReadable and FsObj::isWritable methods, respectively.

\note The values of the permissions for a file or directory are cached in their
      instances, so, if there's a need to get the latest values for the
      permissions, remember to call their \ref FsObj::refresh "refresh" methods
      before the intended operations.

\subsubsection usestat Statting an object

Like common *POSIX* filesystems, File and Dir objects have a \ref FsObj::stat
"stat" method for getting information about them.

There are two ways of statting objects in *libradosfs*, from their very instance:

    radosfs::File file(&fs, "/my-file");

    struct stat fileStat;
    file.stat(&fileStat);

 or from the Filesystem instance:

    struct stat fileStat;
    fs.stat("/my-file", &fileStat);

Both methods should essencially be the same, however, if the File or Dir
instances already exist, then it might be faster to use them instead of the
Filesystem.

\subsubsection useparallelstat Statting many objects

A common use-case is to list a directory's contents in a detailed way, printing
information about the permissions, ownership and size of each entry.
To accomplish this, an obvious way is to stat each entry individually and print
the information in the end. However, this might become extremely inefficient
for directories with a large number of entries as it involves many IO operations
on the cluster.

As an alternative, there is a \ref Filesystem::stat(const std::vector<std::string>& )
"special method" for statting many paths at a time (not only for paths in the
same directory).

This version of the stat operation optimizes the stat operations as much as it
can. For example, it gathers all the paths that belong to the same parent and
gets information about all of them at the same time, reducing dramatically the
number of IO operations and increasing performance. Besides that, for operations
that need to be done individually (for example, paths belonging to different
parents), they are still performed in parallel.

Here an example of how this method can be used for statting all the entries in
a directory:

    ...
    
    // Get the directory's entries
    std::set<std::string> entries;
    dir.entryList(entries);
    
    // Insert the entries into a vector for Filesystem::stat
    std::vector<std::string> pathsToStat;
    pathsToStat.insert(pathsToStat.begin(), entries.begin(), entries.end());
    
    std::vector<std::pair<int, struct stat> > statResults;
    
    statResults = fs.stat(pathsToStat);
    
    ...

In the example, *statResults* will hold the results of the stat operations
corresponding to the given paths in the same order. Each result is a pair whose
first member is the stat's result code, and the second member is the *stat*
struct with the object's information. Thus, the stat information should only be
considered for pairs whose result code is a **0**.

\subsection usedir Directories

Directories are represented by the Dir class which, just like the File class,
derive from FsObj.

The Dir class can be used for creating/removing a directory, listing it,
change its permissions, etc.

\subsubsection uselistdir Listing a directory

Listing a directory can be done using its Dir::entryList method. For example:

    std::set<std::string> entries;
    
    radosfs::Dir dir(&fs, "/path/to/dir/");
    dir.entryList(entries);

\see Dir::entry for an alternative way of getting an entry from a directory.

\note Listing a directory with Dir::entryList uses the last entry list read from
      the directory, which might be outdated depending on the use-case and the
      filesystem use. Remember to call Dir::refresh before listing the entries if
      there is a need for getting the updated list of entries.

\subsubsection usefindindir Finding contents in a directory

There is a Dir::find method for finding contents (matching a given criteria)
inside a directory (and recursively inside its subdirectories).

The criteria for matching contents is specified as key/value pairs in a string
argument. Even though the method blocks until the operation is finished,
internally the operation is performed asynchronously and in parallel leveraging
Rados's asynchronous API and *libradosfs*'s
[generic worker threads](\ref genericworkers).

Example:

    std::set<std::string> foundItems;
    dir.find("name = '^my-.*' xattr != "important" size <= 1024", foundItems);

The example above would find all files and directories whose name starts with
*my-*, size is lesser than or equal to 1 KB and does not have an xattribute
called *important*.

Here is a list of the keywords supported by the find operation:


Keyword | Example                          | Action                                          | Supp. operators     | Expected values
--------|----------------------------------|-------------------------------------------------|---------------------|--------------------------------------------------------------------------------------
name    | name = 'my-file'                 | check name                                      | =, !=               | a string with a name or regular expression
iname   | iname = '.+\\\\\.bak'            | check case insensitive name                     | =, !=               | same as *name* but matches case insensitive names
size    | size = 100                       | check size                                      | =, !=, <, <=, >, >= | a numeric value
uid     | uid = 0                          | check UID                                       | =, !=, <, <=, >, >= | a numeric value
gid     | gid = 0                          | check GID                                       | =, !=, <, <=, >, >= | a numeric value
xattr   | xattr = 'last-read'              | check xattr key                                 | =, !=               | a string with a name or regular expression
xattr.  | xattr.'last-read' = '20150425'   | check value of the given xattr                  | =, !=, <, <=, >, >= | a key name as part of the argument and a value as a name or regular expression
ixattr  | ixattr = 'User'                  | check case insensitive xattr key                | =, !=               | same as *xattr* but matches case insensitive keys
ixattr. | ixattr.'user' = 'Jay'            | check case insensitive value of the given xattr | =, !=               | same as *xattr.* but matches case insensitive values
xattrN. | xattrN.'last-read' > '20150000'  | check numeric value of xattr named KEY          | =, !=, <, <=, >, >= | a key name as part of the argument and a numeric value
mtd     | mtd = 'complete'                 | check metadata key                              | =, !=               | a string with a name or regular expression
mtd.    | mtd.'complete' = '0.80'          | check value of metadata named KEY               | =, !=, <, <=, >, >= | a key name as part of the argument and a value as a name or regular expression
imtd    | imtd = 'Month'                   | check case insensitive xattr key                | =, !=               | same as *mtd* but matches case insensitive keys
imtd.   | imtd.'month' = 'August'          | check case insensitive value of the given xattr | =, !=               | same as *mtd.* but matches case insensitive values
mtdN.   | mtdN.'complete' < '1'            | check numeric value of metadata named KEY       | =, !=, <, <=, >, >= | a key name as part of the argument and a numeric value

\note For the *xattr.KEY* and *mtd.KEY* cases, if the operators = or != are
      used, then it compares the *string* value of the xattribute, if other
      operators are used, then it compares the value as a *float* number.

\note The supported regular expressions syntax is the *POSIX Basic Regular
      Expression* one.

\subsubsection usetmid TMId (Transversal Modification Id)

Directories support a *Transversal Modification Id* which is a unique id set
when any of the directories' children is modified.

This can be useful to e.g. quickly detect any modifications in a directory
tree.

The *TMId* is only used if the directory has that setting. For checking
whether it is set on a directory, use the Dir::usingTMId method. For modifying
this setting, use Dir::useTMId. By default, directories do not use *TMId*.

For getting the value of the *TMId* use the Dir::getTMId method.

\note Keep in mind that in order for the *TMId* to be propagated up in the
      hierarchy of directories, all the directories in the path need to be using
      it too.

\subsubsection usedircache Cacheable directories

For most use-cases, using the simple
Dir::Dir(Filesystem* radosFs, const std::string& path) constructor is the
recommended thing to do as it will cache the directories and improve the
performance when, e.g. listing them.
For some other use-cases however, the contrary might be true if you're using
directories in more complex use-cases, be sure to read the \ref dircache section
of the \ref arch page.


\subsection usefile Files

Files are represented by the File class which, as mentioned before, derives from
FsObj, sharing a part of its interface with Dir.

The File class is used for the common file operations in *libradosfs*,
creating/removing, writing, reading, truncating, etc.

\subsubsection usefileinstancing Instanciating a File object

Although File objects have no *open* methods, they do have an *open mode*:
File::OpenMode. This mode is used to control (besides the file's permissions)
what can be done in the file as an extra protection since it restricts which
operations are allowed in the File.
When the mode is not specified, File::OpenMode::MODE_READ_WRITE is used.

\subsubsection usefilereadsync Reading a file synchronously

The most common way of reading a file is by using the synchronous method
File::read method with an offset and length. For example:

    radosfs::File file(&fs, "/readable/file-path", File::MODE_READ);
    
    char buffer[256];
    ssize_t numBytesRead = file.read(buffer, 0, 256);
    
In the example, the *read* operation will try to read 255 bytes from the
beginning (offset 0) of the file and returns the number of bytes effectively
read.

\subsubsection usefilereadasync Reading a file asynchronously

For some use-cases, like reading a large file, reading portions of the file in
parallel and asynchronously may be very important as it may deeply improve the
speed. That can be accomplished by the \ref File::read(const std::vector<FileReadData> &, std::string *, AsyncOpCallback, void *)
"File::read" overloaded method.

Let's consider as a simple example reading 1 MB from a file in the mentioned
way:

    const size_t dataSize = 1024 * 1024;
    const size_t readSize = dataSize / 4;
    char *buffer = new char[dataSize];
    ssize_t opsResult[4];
    std::vector<radosfs::ReadData> intervals;
    
    for (size_t i = 0; i < 4; i++)
    {
        radosfs::ReadData readData(buffer + i * readSize,
                                   i * readSize,
                                   readSize,
                                   &operationsResult[i]);
        intervals.push_back(readData);
    }
    
    file.read(intervals);
    file.sync();
    
    ...

The example above reads 1 MB from *file* in 256 KB portions which are read in
parallel.
The *opsResult* array would keep the result of each of the 4 read operations
operations performed, that is, the length of the data read or an error code.
Since the method is asynchronous, File::sync is called to make sure the method
is finished (checkout the @ref File::read(const std::vector<FileReadData> &, std::string *, AsyncOpCallback, void *)
"documentation" for more information on how to use the method).

\subsubsection usefilewritesync Writing a file synchronously

Similar to File::read, there is a File::writeSync method for writing a buffer
of data to a file using an offset and a length. For example:

    radosfs::File file(&fs, "/writable/file-path");
    
    int opResult = file.writeSync("CERN", 0, 4);

Unlike File::read, the File::writeSync method does not return the number of
bytes written, as it successfully writes all the given length of the buffer,
returning 0, or returns an error code otherwise.

\subsubsection usefilewriteasync Writing a file asynchronously

Writing a file asynchronously is possible using the File::write method and is
very similar to the synchronous version used in the \ref usefilewritesync
"previous section"'s example. One important things should be kept in mind
though: if the scope of the buffer to be written is local, then, pass the
*copyBuffer* argument as *true*.


\subsection useinode File inodes

Files and directories both have inode objects which is, essencially, where their
data is stored. (See the \ref fileinode section in the \reg arch page for more
information)

In the case of file objects, there might be use-cases where it's convenient to
add the data to a file before the file is visible or available in the system.
The FileInode class can be used to accomplish this.

Reading the \ref FileInode API docs "FileInode API docs", it is clear how its
interface is very similar to the File's (in fact, the File class uses the
FileInode internally).
Nonetheless, the way a FileInode is constructed differs a little bit.
FileInode objects have a name (not a path) and it is extremely important that
no inodes have the same name. That is why FileInode objects generate a *uuid*
for their name so you should very likely always use one of the constructors that
automatically assigns its object's name. Nonetheless, for flexibility purposes,
there is still the option of providing a FileInode object with a given name.

Since FileInode objects have no path, a pool's name needs to be provided when
instantiating them (because, without the path, it is not possible to understand
which data pool should be used).

When all the changes have been done to a file inode, it can be made available to
the filesystem by registering it with a file path using FileInode::registerFile.

Here is an example of how to use a FileInode:

    ...
    radosfs::FileInode inode(&fs, "data-pool");

    // write data or truncate...

    inode.registerFile("/my-file", 1000, 1000);

    ...


\subsection uselinks Links

*libradosfs* supports symbolic links to File and Dir objects (note that a link to a
link is not allowed).

Links can be created using the FsObj::createLink method, for example:

    ...
    radosfs::Dir dir(&fs, "/tmp/");
    dir.createLink("/TMP/");
    ...

When a link exists, it can be instantiated the usual way, for example:

    ...
    radosfs::Dir tmpDir(&fs, "/TMP/");
    ...

As expected, creating an object inside a link directory will actually do it in
the link's target directory. Analogously, writing to a file who is a link will,
in fact, write to the link's target file.

It is possible to check if an FsObj is a link by using the FsObj::isLink method
and to get its target path by using FsObj::targetPath, for example:

    ...
    radosfs::FsObj *obj = fs.getFsObj("/TMP/");

    if (obj && obj->isLink())
    {
        printf("%s is a link to %s", obj->path.c_str(), obj->targetPath().c_str());
    }

    ...


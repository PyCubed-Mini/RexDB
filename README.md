# RexDB

A very simple Python database with time as the primary method of querying.

## How it works.

rexDB works in a very straightforward manner. It works through the operating system file structure. The database is stored in a directory called db\_<number>, this is so that multiple databases could be stored in the same directory. inside the database folder is another set of folders and within those folders are the files that contain your entries. However, these files are unreadable as they are just structs packed into bytes.
Each folder has a special file called a map, this map stores the start and end time of each file within the folder. This lets the query manager very easily ascertain if a given entry will be within a folder and if it is within a folder, which file it is in. This makes querying based on time much faster than querying based on other fields in the database.

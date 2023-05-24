# RexDB

A very simple Python database with time as the primary method of querying.

## How it works.

rexDB works in a very straightforward manner. It works through the operating system file structure. The database is stored in a directory called db\_<number>, this is so that multiple databases could be stored in the same directory. inside the database folder is another set of folders and within those folders are the files that contain your entries. However, these files are unreadable as they are just structs packed into bytes.
Each folder has a special file called a map, this map stores the start and end time of each file within the folder. This lets the query manager very easily ascertain if a given entry will be within a folder and if it is within a folder, which file it is in. This makes querying based on time much faster than querying based on other fields in the database.

## Methods

### Constructor (**init**)

**required arguments**

- fstring
  - string
  - the format string for your data
  - if you don't know how packing structs work in Python take a look at Python's documentation [here](https://docs.python.org/3/library/struct.html#format-characters)
- field_names
  - string tuple
  - a tuple containing all the names of the fields in your database
  - the order _must_ match your format string

**optional arguments**

- bytes_per_file
  - integer
  - you can use this to specify how many bytes you want per file, if that is a constraint for your application
  - the default is 1KB.
- files_per_folder
  - integer
  - you can use this to specify how many files you would like in each folder that the database creates if this is a constraint for your application
  - the default is 50 files per folder (50 KB per folder)
- time_method
  - None -> time.struct_time
  - a function that will give the timestamps you would like to use in your database. The default is Python's time.gmtime()
  - This function _must return a time.struct\_time_. _NOT_ a float containing seconds since epoch.
  - Because of the use of the time.struct\_time datatype, timestamps will only have precision down to the nearest second, if your application requires more precision than that do not use this database.

This function will initialize an empty database that you can then use to store your entries. 

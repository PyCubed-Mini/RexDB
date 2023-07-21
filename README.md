# RexDB

A very simple Python database with time as the primary method of querying.

## table of contents
- [RexDB](#rexdb)
  - [table of contents](#table-of-contents)
  - [How it works.](#how-it-works)
  - [Methods](#methods)
    - [**Constructor** (\_\_init\_\_)](#constructor-__init__)
    - [**log**](#log)
    - [**get\_data\_at\_time**](#get_data_at_time)
    - [**get\_data\_at\_range**](#get_data_at_range)
## How it works.

RexDB works in a very straightforward manner. It works through the operating system file structure. The database is stored in a directory called db\_\<number\>, this is so that multiple databases could be stored in the same directory. inside the database folder is another set of folders and within those folders are the files that contain your entries. However, these files are unreadable as they are just structs packed into bytes.
Each folder has a special file called a map, this map stores the start and end time of each file within the folder. This lets the query manager very easily ascertain if a given entry will be within a folder and if it is within a folder, which file it is in. This makes querying based on time much faster than querying based on other fields in the database.

## Methods

### **Constructor** (\_\_init\_\_)

<u>type</u>

- `None -> rexDB`

<u>arguments</u>

- `fstring`
  - `string`
  - the format string for your data
  - if you don't know how packing structs work in Python take a look at Python's documentation [here](https://docs.python.org/3/library/struct.html#format-characters)
- `field_names`
  - `string tuple`
  - a tuple containing all the names of the fields in your database
  - the order _must_ match your format string
- `bytes_per_file`
  - `integer`
  - you can use this to specify how many bytes you want per file, if that is a constraint for your application
  - the default is 1KB.
- `files_per_folder`
  - `integer`
  - you can use this to specify how many files you would like in each folder that the database creates if this is a constraint for your application
  - the default is 50 files per folder (50 KB per folder)
- `time_method`
  - `None -> time.struct_time`
  - a function that will give the timestamps you would like to use in your database. The default is Python's `time.gmtime()`
  - This function _must return a_ `time.struct_time`. _NOT_ a float containing seconds since epoch.
  - Because of the use of the time.struct\_time datatype, timestamps will only have precision down to the nearest second, if your application requires more precision than that do not use this database.
- `filepath`
  - `string`
  - the directory you want your database to go in
- `new_db`
  - `bool`
  - If the database should be a new instance of a database or if it should attempt to find an existing database in the current directory and continue the existing database
  - If `new_db` is true, `fstring`, `field_names`, `bytes_per_file`, and `files_per_folder` will all be overwritten with what is found in the existing database at the filepath given. 

<u>functionality</u>

The \_\_init\_\_ function will initialize an empty database that stores data specified by the given string format. Data will also be kept track of based on the field names that you specify. 

### **log**

<u>type</u>

- `tuple -> bool`

<u>arguments</u>

- data
  - tuple
  - a tuple containing the data you want to log in the database
  - the tuple must be entered in the same order you specified in your format string and field names
  - if you are storing characters or strings, you must use a 'b' string to specify that it is a byte string or convert it to bytes using the builtin Python "bytes" function and utf-8 encoding.

<u>functionality</u>

Will log your data in the database and mark it with an automatically generated time stamp. You will be able to query on this timestamp later. The function will return `True` if logging was successful and `False` otherwise. 

### **get_data_at_time**

<u>type</u>

- `time.struct_time -> tuple`

<u>arguments</u>

- time
  - time.struct_time
  - the time of the entry which you want to retrieve. 

<u>functionality</u>

Given a time, this function will return the data entry logged at that time. If there are multiple entries at the specified time, it will return the first of those entries that was logged. 

### **get_data_at_range**

<u>type</u>

- `time.struct_time * time.struct_time -> tuple list`

<u>arguments</u>

- start_time
  - time.struct_time
  - the start of your specified range
- end_time
  - time.struct_time
  - the end of your specified range

<u>functionality</u>

Will return all entries within a specified time range, if there are no entries within the specified range, will return an empty list.
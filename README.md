#distributed-file-monitor-java6#

##TODO##
1. make concurrent inserts possible without duplication - DONE
2. on start-up scan the monitored folder for files and check if they exist in the database, if not add them
3. enhance the database table with created\_by and created\_on - DONE
3. think of faiure scenarios
4. investigate why versioning is not working (is versioning really needed???) - not needed -
5. redirect standard output to target/watcher.log - DONE
6. fix checkstyle and findbugs warnnings and make them part of the build process

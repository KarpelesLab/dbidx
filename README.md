# dbidx

Database indexing low level constructs. This can be used to index values of different kinds in a keyval store, allowing fetching based on specific values, check for unique entries or sorting.

The basic process is to generate index values for a record and store these alongside the record. If the record is modified the index values need to be modified. It is up to the store to keep track of which index values exist for a record and remove the old values.

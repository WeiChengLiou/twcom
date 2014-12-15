This is the data file created by `mongodump`. Please restore the mongodb by following steps:

```sh
7z x twcom.7z.001 # this should create a directory named dump
mongorestore dump/ # this should restore the mongo database "twcom" in local mongodb
```

The restored mongo database should be looked like this:

```
> show dbs
local	0.078125GB
twcom	3.9521484375GB
```

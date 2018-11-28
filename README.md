# MDB
mysql operating class for python, using like thinkphp

## Setting 

Config the database information
> cp default.yaml.demo default.yaml

Install the denpendency
> pip install torndb
> pip install yaml

## Using 
The CURD of MDB 
```buildoutcfg   
import MDB
mdb = MDB.DB('./default.yaml')
user={
    "name" : "neo",
    "age" : 11,
}
res = mdb.talbe("user").add(user)
res = mdb.talbe("user").where({"user_id":1}).delete()
res = mdb.talbe("user").where({"user_id":1}).find()
res = mdb.talbe("user").where({"user_id":1}).save(user)
```

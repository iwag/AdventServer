
build & run
==========

```
$ sbt 'run-main com.github.iwag.StoreServer -admin.port=:49991 -bind=:49091'
$ sbt 'run-main com.github.iwag.SearchServer -admin.port=:49992 -bind=:49092'
$ sbt 'run-main com.github.iwag.CacheServer -admin.port=:49993 -bind=:49093'
$ sbt 'run-main com.github.iwag.HTTPServer -admin.port=:49990 -http=:48000 -search=:49092 -cache=:49093 -db=:49091'
$
$ curl -XPOST http://localhost:48000/ -d'hello world'
OK
$ curl -XPOST http://localhost:48000/ -d'世界世界'
OK
$ curl -XPOST http://localhost:48000/ -d'hello 世界'
OK
$ curl -XPOST http://localhost:48000/ -d'こんにちわ'
OK
$ curl -XPOST http://localhost:48000/ -d'こんにちわ世界'
OK
$ curl http://localhost:48000/hello
["hello 世界","hello hello"]
$ curl http://localhost:48000/%E4%B8%96%E7%95%8C
["hello 世界","こんにちわ世界","世界世界"]
```

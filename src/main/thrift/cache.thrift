namespace java com.github.iwag

struct CacheResponse {
1:    optional string hit
2:    string msg
}

service CacheService {
  CacheResponse get(1: string key)

  void put(1: string key, 2: string value)
}

exception BaseException {
1: string message
}

service SearchService {
  list<i32> search(1: string key)

  i32 put(1: string value)

  void delete(1: i32 id) throws (1: BaseException exp)
}

service StoreService {
  string get(1: i32 key) throws (1: BaseException exp)

  void put(1: i32 id, 2: string value)

  void delete(1: i32 id) throws (1: BaseException exp)
}

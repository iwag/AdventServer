namespace java com.github.iwag

struct CacheResponse {
1:    optional string hit
2:    string msg
}

service CacheService {
  CacheResponse get(1: string key)

  void put(1: string key, 2: string value)
}

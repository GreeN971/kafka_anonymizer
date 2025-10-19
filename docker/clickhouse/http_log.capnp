#hash generated via capnp command in console
@0xc46b3d6931071ba4;

struct HttpLogRecord {
  timestampEpochMilli @0 :UInt64;
  #Časová stopa kdy nastal HTTP požadavek

  resourceId @1 :UInt64;
  #Číslo identifikující zdroj

  bytesSent @2 :UInt64;
  #Počet bajtů v rámci požadavku

  requestTimeMilli @3 :UInt64;
  #Čas do naplnění požadavku

  responseStatus @4 :UInt16;
  #Kód HTTP odpovědi

  cacheStatus @5 :Text;
  #Status mezipěmti HIT nebo MISS...

  method @6 :Text;
  #HTTP metoda do clickHouse

  remoteAddr @7 :Text; #IP format
  #Adresa určená pro anonymizaci

  url @8 :Text;
  #TO DO 
}

#The Kafka table engine doesn't support columns with default value. 
# If you need columns with default value, you can add them at materialized view level (see below).
# This may be issue since I have the list which is for is not in the Clickhouse schema
struct HTTPLogsList {
    logs @0 : List(HttpLogRecord);
}

#maybe add default values to all since it can be easier to debug, but It needs to documented
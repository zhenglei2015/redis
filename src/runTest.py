import redis
from random import Random

def random_str(randomlength=8):
    str = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        str+=chars[random.randint(0, length)]
    return str

keys = []

r = redis.StrictRedis(host='localhost', port=6379, db=0)


key = random_str(20)
for x in range(100000):
  if x % 50000 == 0:  
    key = random_str(20)
  val = random_str(20)
  rk = key + "." + val
  if x % 1000 == 0:
    print rk
  keys.append(rk)
  r.set(rk, "xxxxxxxxxxxxxxxxxxxx")

for key in keys:
  r.delete(key)

#print "deleted all keys"
print "OK"
  


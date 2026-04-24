import hashlib

word = "1234"
md5_hash = hashlib.md5(word.encode('utf-8')).hexdigest()

print(f"Слово: {word}")
print(f"MD5 хэш: {md5_hash}")
### Root CA

```bash
openssl genrsa -des3 -out myCA.key 4096
openssl req -subj "/CN=localhost" -key myCA.key -days 36500 -nodes -x509 -keyout myCA.key -out myCA.crt
```

### Server

```bash
openssl req -nodes -subj "/CN=localhost.localdomain" -new -newkey rsa:2048 -keyout server.key -out server.csr
openssl x509 -req -in server.csr -CA myCA.crt -CAkey myCA.key -CAcreateserial -out server.crt -days 36500
```

### Client

```bash
openssl req -nodes -subj "/CN=me" -newkey rsa:2048 -keyout client.key -out client.csr
openssl x509 -req -in client.csr -out client.crt -CAcreateserial -CA myCA.crt -CAkey myCA.key -days 36500
```

### Some_user

```bash
openssl req -nodes -subj "/CN=some_user" -newkey rsa:2048 -keyout some_user.key -out some_user.csr
openssl x509 -req -in some_user.csr -out some_user.crt -CAcreateserial -CA marsnet_ca.crt -CAkey marsnet_ca.key -days 36500

```

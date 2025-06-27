


## How to work with self-signed certificates 

1. Create a self-signed Root CA. This should be used for signing node certificates and adding to a truststore
2. Create node self-signed certificates 
3. Sign node certificates with root CA 

Create root key for CA cert
```shell
openssl genrsa -out rootCA.key 4096
```

Create root CA 
```shell
openssl req -x509 -new -nodes -key rootCA.key -sha256 -days 65535 -out rootCA.crt
```


Create node certificates 
```shell

openssl req -x509 -nodes -days 65536 -newkey rsa:2048 -keyout node1.key -out node1.crt -subj "/CN=node1.test" -addext "subjectAltName=DNS:node1.test"
 
openssl req -x509 -nodes -days 65536 -newkey rsa:2048 -keyout node2.key -out node2.crt -subj "/CN=node2.test" -addext "subjectAltName=DNS:node2.test"
```

```shell
openssl x509 -req -in node1.crt -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -out node1-signed.crt -days 65536 -sha256

openssl x509 -req -in node2.crt -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -out node2-signed.crt -days 65536 -sha256
```



### Useful commands 

```shell
keytool -printcert -file rootCA.crt # read content of the certificate 
```
FROM minimal-ubuntu

ENV DEBIAN_FRONTEND noninteractive

WORKDIR /root/

RUN apt update -y
RUN apt install -y python3-dev python3-pip
RUN apt install -y ntp krb5-admin-server krb5-kdc
RUN pip install supervisor

# remove default configuration
RUN rm -r /var/lib/krb5kdc/
RUN rm -r /etc/krb5kdc/

# python web server configuration
COPY ./index.html .

# kerberos server configuration
ENV KRB5_CONFIG=/etc/krb5.conf
ENV KRB5_KDC_PROFILE=/var/kerberos/krb5kdc/kdc.conf
RUN mkdir -pv /var/kerberos/krb5kdc
COPY ./kdc.conf /var/kerberos/krb5kdc/kdc.conf
COPY ./kadm5.acl /var/kerberos/krb5kdc/kadm5.acl
COPY ./krb5.conf /etc/krb5.conf
RUN mkdir -pv /var/log/kerberos/
RUN touch /var/log/kerberos/krb5.log
RUN touch /var/log/kerberos/kadmin.log
RUN touch /var/log/kerberos/krb5lib.log
RUN kdb5_util -r EXAMPLE.COM -P krb5 create -s

RUN mkdir -p /var/log/supervisord/
COPY ./supervisord.conf /etc/supervisord.conf

# when container is starting
CMD ["/usr/local/bin/supervisord", "-n", "-c", "/etc/supervisord.conf"]

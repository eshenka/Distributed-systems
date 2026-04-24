FROM debian
MAINTAINER Pragmatic Programmer
RUN apt-get update
RUN apt-get install nginx -y
VOLUME "/var/www/html"
EXPOSE 80
CMD /usr/sbin/nginx -g "daemon off;"

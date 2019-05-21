FROM  himuradba/centos

#dmshttp
RUN mkdir -p /opt/dmshttp
ADD . /opt/dmshttp
RUN cd /opt/dmshttp \
    && pip3 install -r /opt/dmshttp/requirements.txt -i https://mirrors.ustc.edu.cn/pypi/web/simple/ \
    && sed -i "178s/^/#/" /usr/local/python3/lib/python3.7/site-packages/mybatis_mapper2sql/convert.py \
    && sed -i "72s/basestring, unicode/str/" /usr/local/python3/lib/python3.7/site-packages/schemaobject/connection.py \
    && ln -fs /usr/local/python3/bin/supervisord /usr/bin/supervisord \
    && ln -fs /usr/local/python3/bin/supervisorctl /usr/bin/supervisorctl

#port
EXPOSE 8868

#start service
ENTRYPOINT ["supervisord", "-c" ,"/opt/dmshttp/supervisord.conf"]

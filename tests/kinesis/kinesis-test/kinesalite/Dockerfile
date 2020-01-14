FROM node:8.14.0-alpine

RUN apk add --no-cache make gcc g++ python

USER node

RUN mkdir /home/node/.npm-global

WORKDIR /home/node

ENV PATH=/home/node/.npm-global/bin:$PATH
ENV NPM_CONFIG_PREFIX=/home/node/.npm-global

RUN npm install -g kinesalite@1.14.0

CMD ["kinesalite", "--port", "4567", "--ssl", "true", "--shardLimit", "10"]

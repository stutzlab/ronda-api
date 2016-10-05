FROM node:4.4.5

RUN mkdir -p /app/src
WORKDIR /app

COPY package.json /app/
RUN npm install

COPY /src/* /app/src/

EXPOSE 80

CMD ["npm", "start"]

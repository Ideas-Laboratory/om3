FROM node:18-alpine

WORKDIR /app

COPY . .

RUN npm install

ENV DOCKER_ENV=docker_env 
ENV POSTGRES_HOST=om3_db
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=om3dbpassword
ENV POSTGRES_DB=linesdata

CMD npm run start

EXPOSE 3000 3001
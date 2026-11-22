FROM node:20-alpine

WORKDIR /app

# Install git for git dependencies if needed
RUN apk add --no-cache git

COPY package*.json ./

RUN npm install --allow-git=all

COPY . .

EXPOSE 3001
ENV PORT=3001

CMD ["node", "server.js"]

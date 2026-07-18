FROM node:22-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    fuse3 \
    git \
    python3 \
    make \
    g++ \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# The Node base image already includes matching headers; keep native addon
# builds offline instead of downloading them through deployment proxies.
ENV npm_config_nodedir=/usr/local

COPY package.json package-lock.json tsconfig.json ./
RUN npm ci

COPY src/ src/
RUN npx tsc

COPY bin/agentfs /usr/local/bin/agentfs
RUN chmod +x /usr/local/bin/agentfs

EXPOSE 4000 4001 4002 5000-5015

CMD ["node", "dist/command-server.js"]

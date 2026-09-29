FROM node:22-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY hub.js ./
ENV PORT=8787
EXPOSE 8787
CMD ["node", "hub.js"]

FROM public.ecr.aws/docker/library/node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY tsconfig.json ./
COPY src ./src
COPY data ./data
RUN npm run build
EXPOSE 3030
CMD ["npm","start"]

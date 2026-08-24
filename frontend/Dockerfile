FROM docker.io/library/node:24-alpine AS build

WORKDIR /workspace
RUN npm install --global pnpm@11.22.0

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY . .
ARG VITE_USE_FIXTURES=false
ENV VITE_USE_FIXTURES=${VITE_USE_FIXTURES}
RUN pnpm build

FROM docker.io/nginxinc/nginx-unprivileged:1.29.4-alpine
LABEL org.opencontainers.image.source="https://github.com/kl3inIT/kdlstc"
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /workspace/dist /usr/share/nginx/html

EXPOSE 8080

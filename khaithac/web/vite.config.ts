import react from "@vitejs/plugin-react"
import tailwindcss from "@tailwindcss/vite"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"

// Build xuất thẳng vào static/ của Spring -> mọi thứ chạy trên 8090 (BFF giữ nguyên).
// Dev: `pnpm dev` trên 5173, proxy giữ nguyên Host để redirect OIDC quay về 5173.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  build: {
    outDir: "../src/main/resources/static",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: Object.fromEntries(
      ["/api", "/oauth2", "/login", "/logout"].map((p) => [
        p,
        { target: "http://localhost:8090", changeOrigin: false },
      ]),
    ),
  },
})

import react from "@vitejs/plugin-react"
import tailwindcss from "@tailwindcss/vite"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
  },
  build: {
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            {
              name: "vendor-framework",
              test: /node_modules[\\/](react|react-dom|react-router|@refinedev[\\/](core|react-router))[\\/]/,
            },
          ],
        },
      },
    },
  },
  server: {
    port: 5174,
    proxy: Object.fromEntries(
      ["/api", "/oauth2", "^/login/oauth2/", "/logout"].map((path) => [
        path,
        { target: "http://localhost:8080", changeOrigin: false },
      ]),
    ),
  },
})
